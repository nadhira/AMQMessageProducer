import logging

from twisted.internet import reactor, defer, task
from twisted.trial import unittest

from stompest import async, sync
from stompest.async.util import sendToErrorDestinationAndRaise
from stompest.config import StompConfig
from stompest.error import StompConnectionError, StompProtocolError
from stompest.protocol import StompSpec, commands

logging.basicConfig(level=logging.DEBUG)
LOG_CATEGORY = __name__

from . import HOST, PORT, VERSION, LOGIN, PASSCODE, VIRTUALHOST, BROKER

class StompestTestError(Exception):
    pass

class AsyncClientBaseTestCase(unittest.TestCase):
    queue = None
    errorQueue = None
    log = logging.getLogger(LOG_CATEGORY)
    headers = {StompSpec.ID_HEADER: '4711'}

    TIMEOUT = 0.2

    def getConfig(self, version, port=None):
        return StompConfig('tcp://%s:%s' % (HOST, port or PORT), login=LOGIN, passcode=PASSCODE, version=version)

    def cleanQueues(self):
        self.cleanQueue(self.queue)
        self.cleanQueue(self.errorQueue)

    def cleanQueue(self, destination, headers=None):
        if not destination:
            return

        client = sync.Stomp(self.getConfig(StompSpec.VERSION_1_0))
        client.connect(host=VIRTUALHOST)
        client.subscribe(destination, headers)
        while client.canRead(0.2):
            frame = client.receiveFrame()
            self.log.debug('Dequeued old %s' % frame.info())
        client.disconnect()

    def setUp(self):
        self.cleanQueues()
        self.unhandledFrame = None
        self.errorQueueFrame = None
        self.consumedFrame = None
        self.framesHandled = 0

    def _saveFrameAndBarf(self, _, frame):
        self.log.info('Save message and barf')
        self.unhandledFrame = frame
        raise StompestTestError('this is a test')

    def _barfOneEatOneAndDisonnect(self, client, frame):
        self.framesHandled += 1
        if self.framesHandled == 1:
            self._saveFrameAndBarf(client, frame)
        self._eatOneFrameAndDisconnect(client, frame)

    def _eatOneFrameAndDisconnect(self, client, frame):
        self.log.debug('Eat message and disconnect')
        self.consumedFrame = frame
        client.disconnect()

    def _saveErrorFrameAndDisconnect(self, client, frame):
        self.log.debug('Save error message and disconnect')
        self.errorQueueFrame = frame
        client.disconnect()

    def _eatFrame(self, client, frame):
        self.log.debug('Eat message')
        self.consumedFrame = frame
        self.framesHandled += 1

    def _nackFrame(self, client, frame):
        self.log.debug('NACK message')
        self.consumedFrame = frame
        self.framesHandled += 1
        client.nack(frame)

    def _onMessageFailedSendToErrorDestinationAndRaise(self, client, failure, frame, errorDestination):
        sendToErrorDestinationAndRaise(client, failure, frame, errorDestination)

class HandlerExceptionWithErrorQueueIntegrationTestCase(AsyncClientBaseTestCase):
    frame1 = 'choke on this'
    msg1Hdrs = {'food': 'barf', 'persistent': 'true'}
    frame2 = 'follow up message'
    queue = '/queue/asyncHandlerExceptionWithErrorQueueUnitTest'
    errorQueue = '/queue/zzz.error.asyncStompestHandlerExceptionWithErrorQueueUnitTest'

    def test_onhandlerException_ackMessage_filterReservedHdrs_send2ErrorQ_and_disconnect_version_1_0(self):
        self.cleanQueue(self.queue)
        return self._test_onhandlerException_ackMessage_filterReservedHdrs_send2ErrorQ_and_disconnect('1.0')

    def test_onhandlerException_ackMessage_filterReservedHdrs_send2ErrorQ_and_disconnect_version_1_1(self):
        self.cleanQueue(self.queue, self.headers)
        return self._test_onhandlerException_ackMessage_filterReservedHdrs_send2ErrorQ_and_disconnect('1.1')

    @defer.inlineCallbacks
    def _test_onhandlerException_ackMessage_filterReservedHdrs_send2ErrorQ_and_disconnect(self, version):
        if version not in commands.versions(VERSION):
            print 'This broker does not support STOMP protocol version 1.1'
            return

        config = self.getConfig(StompSpec.VERSION_1_1)
        client = async.Stomp(config)

        try:
            client = yield client.connect(host=VIRTUALHOST)
            if client.session.version == '1.0':
                yield client.disconnect()
                raise StompProtocolError('Broker chose STOMP protocol 1.0')

        except StompProtocolError as e:
            print 'Broker does not support STOMP protocol 1.1. Skipping this test case. [%s]' % e
            defer.returnValue(None)

        if client.session.version != version:
            print 'Broker does not support STOMP protocol %s. Skipping this test case.' % version
            yield client.disconnect()
            defer.returnValue(None)

        # enqueue two messages
        client.send(self.queue, self.frame1, self.msg1Hdrs)
        client.send(self.queue, self.frame2)

        defaultHeaders = {StompSpec.ACK_HEADER: 'client-individual'}
        if version != '1.0':
            defaultHeaders.update(self.headers)

        # barf on first message so it will get put in error queue
        # use selector to guarantee message order.  AMQ doesn't guarantee order by default
        headers = {'selector': "food = 'barf'"}
        headers.update(defaultHeaders)
        client.subscribe(self.queue, self._saveFrameAndBarf, headers, errorDestination=self.errorQueue, onMessageFailed=self._onMessageFailedSendToErrorDestinationAndRaise)

        # client disconnected and returned error
        try:
            yield client.disconnected
        except StompestTestError:
            pass
        else:
            raise

        client = async.Stomp(config) # take a fresh client to prevent replay (we were disconnected by an error)

        # reconnect and subscribe again - consuming second message then disconnecting
        client = yield client.connect(host=VIRTUALHOST)
        headers.pop('selector')
        client.subscribe(self.queue, self._eatOneFrameAndDisconnect, headers, errorDestination=self.errorQueue)

        # client disconnects without error
        yield client.disconnected

        # reconnect and subscribe to error queue
        client = yield client.connect(host=VIRTUALHOST)
        client.subscribe(self.errorQueue, self._saveErrorFrameAndDisconnect, defaultHeaders)

        # wait for disconnect
        yield client.disconnected

        # verify that first message was in error queue
        self.assertEquals(self.frame1, self.errorQueueFrame.body)
        self.assertEquals(self.msg1Hdrs['food'], self.errorQueueFrame.headers['food'])
        self.assertNotEquals(self.unhandledFrame.headers['message-id'], self.errorQueueFrame.headers['message-id'])

        # verify that second message was consumed
        self.assertEquals(self.frame2, self.consumedFrame.body)

    @defer.inlineCallbacks
    def test_onhandlerException_ackMessage_filterReservedHdrs_send2ErrorQ_and_no_disconnect(self):
        config = self.getConfig(StompSpec.VERSION_1_0)
        client = async.Stomp(config)

        # connect
        client = yield client.connect(host=VIRTUALHOST)

        # enqueue two messages
        client.send(self.queue, self.frame1, self.msg1Hdrs)
        client.send(self.queue, self.frame2)

        # barf on first frame, disconnect on second frame
        client.subscribe(self.queue, self._barfOneEatOneAndDisonnect, {StompSpec.ACK_HEADER: 'client-individual'}, errorDestination=self.errorQueue)

        # client disconnects without error
        yield client.disconnected

        # reconnect and subscribe to error queue
        client = yield client.connect(host=VIRTUALHOST)
        client.subscribe(self.errorQueue, self._saveErrorFrameAndDisconnect, {StompSpec.ACK_HEADER: 'client-individual'})

        # wait for disconnect
        yield client.disconnected

        # verify that one message was in error queue (can't guarantee order)
        self.assertNotEquals(None, self.errorQueueFrame)
        self.assertTrue(self.errorQueueFrame.body in (self.frame1, self.frame2))

    @defer.inlineCallbacks
    def test_onhandlerException_disconnect(self):
        config = self.getConfig(StompSpec.VERSION_1_0)
        client = async.Stomp(config)

        yield client.connect(host=VIRTUALHOST)
        client.send(self.queue, self.frame1, self.msg1Hdrs)
        yield client.disconnect()

        yield client.connect(host=VIRTUALHOST)
        # barf on first frame (implicit disconnect)
        client.subscribe(self.queue, self._saveFrameAndBarf, {StompSpec.ACK_HEADER: 'client-individual'}, ack=False, onMessageFailed=self._onMessageFailedSendToErrorDestinationAndRaise)

        # client disconnected and returned error
        try:
            yield client.disconnected
        except StompestTestError:
            pass
        else:
            raise

        # reconnect and subscribe again - consuming retried message and disconnecting
        client = async.Stomp(config) # take a fresh client to prevent replay (we were disconnected by an error)
        client = yield client.connect(host=VIRTUALHOST)
        client.subscribe(self.queue, self._eatOneFrameAndDisconnect, {StompSpec.ACK_HEADER: 'client-individual'})

        # client disconnects without error
        yield client.disconnected

        # verify that message was retried
        self.assertEquals(self.frame1, self.unhandledFrame.body)
        self.assertEquals(self.frame1, self.consumedFrame.body)

class GracefulDisconnectTestCase(AsyncClientBaseTestCase):
    numMsgs = 5
    msgCount = 0
    frame = 'test'
    queue = '/queue/asyncGracefulDisconnectUnitTest'

    @defer.inlineCallbacks
    def test_onDisconnect_waitForOutstandingMessagesToFinish(self):
        config = self.getConfig(StompSpec.VERSION_1_0)
        client = async.Stomp(config, receiptTimeout=1.0)

        # connect
        client = yield client.connect(host=VIRTUALHOST)
        yield task.cooperate(iter([client.send(self.queue, self.frame, receipt='message-%d' % j) for j in xrange(self.numMsgs)])).whenDone()
        client.subscribe(self.queue, self._frameHandler, {StompSpec.ACK_HEADER: 'client-individual'})

        # wait for disconnect
        yield client.disconnected

        # reconnect and subscribe again to make sure that all messages in the queue were ack'ed
        client = yield client.connect(host=VIRTUALHOST)
        self.timeExpired = False
        self.timeoutDelayedCall = reactor.callLater(1, self._timesUp, client) #@UndefinedVariable
        client.subscribe(self.queue, self._eatOneFrameAndDisconnect, {StompSpec.ACK_HEADER: 'client-individual'})

        # wait for disconnect
        yield client.disconnected

        # time should have expired if there were no messages left in the queue
        self.assertTrue(self.timeExpired)

    def _frameHandler(self, client, _):
        self.msgCount += 1
        if self.msgCount < self.numMsgs:
            d = defer.Deferred()
            reactor.callLater(1, d.callback, None) #@UndefinedVariable
            return d
        else:
            client.disconnect(receipt='bye-bye')

    def _timesUp(self, client):
        self.log.debug("Time's up!!!")
        self.timeExpired = True
        client.disconnect()

class SubscribeTestCase(AsyncClientBaseTestCase):
    frame = 'test'
    queue = '/queue/asyncSubscribeTestCase'

    @defer.inlineCallbacks
    def test_unsubscribe(self):
        config = self.getConfig(StompSpec.VERSION_1_0)
        client = async.Stomp(config)

        client = yield client.connect(host=VIRTUALHOST)

        token = yield client.subscribe(self.queue, self._eatFrame, {StompSpec.ACK_HEADER: 'client-individual'})
        client.send(self.queue, self.frame)
        while self.framesHandled != 1:
            yield task.deferLater(reactor, 0.01, lambda: None)

        client.unsubscribe(token)
        client.send(self.queue, self.frame)
        yield task.deferLater(reactor, 0.2, lambda: None)
        self.assertEquals(self.framesHandled, 1)

        client.subscribe(self.queue, self._eatFrame, {StompSpec.ACK_HEADER: 'client-individual'})
        while self.framesHandled != 2:
            yield task.deferLater(reactor, 0.01, lambda: None)
        yield client.disconnect()

    @defer.inlineCallbacks
    def test_replay(self):
        config = self.getConfig(StompSpec.VERSION_1_0)

        client = async.Stomp(config)
        client = yield client.connect(host=VIRTUALHOST)
        client.subscribe(self.queue, self._eatFrame, {StompSpec.ACK_HEADER: 'client-individual'})
        client.send(self.queue, self.frame)
        while self.framesHandled != 1:
            yield task.deferLater(reactor, 0.01, lambda: None)
        client._protocol.loseConnection()
        try:
            yield client.disconnected
        except StompConnectionError:
            pass
        client = yield client.connect(host=VIRTUALHOST)
        client.send(self.queue, self.frame)
        while self.framesHandled != 2:
            yield task.deferLater(reactor, 0.01, lambda: None)

        try:
            yield client.disconnect(failure=RuntimeError('Hi'))
        except RuntimeError as e:
            self.assertEquals(str(e), 'Hi')

        client = yield client.connect(host=VIRTUALHOST)
        client.send(self.queue, self.frame)
        while self.framesHandled != 2:
            yield task.deferLater(reactor, 0.01, lambda: None)

        yield client.disconnect()

class NackTestCase(AsyncClientBaseTestCase):
    frame = 'test'
    queue = '/queue/asyncNackTestCase'

    @defer.inlineCallbacks
    def test_nack(self):
        config = StompConfig(uri='tcp://%s:%d' % (HOST, PORT), version='1.1')
        client = async.Stomp(config)
        try:
            client = yield client.connect(host=VIRTUALHOST)
            if client.session.version == '1.0':
                yield client.disconnect()
                raise StompProtocolError('Broker chose STOMP protocol 1.0')

        except StompProtocolError as e:
            print 'Broker does not support STOMP protocol 1.1. Skipping this test case. [%s]' % e
            defer.returnValue(None)

        client.subscribe(self.queue, self._nackFrame, {StompSpec.ACK_HEADER: 'client-individual', 'id': '4711'}, ack=False)
        client.send(self.queue, self.frame)
        while not self.framesHandled:
            yield task.deferLater(reactor, 0.01, lambda: None)

        yield client.disconnect()

        if BROKER == 'activemq':
            print 'Broker %s by default does not redeliver messages. Will not try and harvest the NACKed message.' % BROKER
            return

        self.framesHandled = 0
        client = yield client.connect(host=VIRTUALHOST)
        client.subscribe(self.queue, self._eatFrame, {StompSpec.ACK_HEADER: 'client-individual', 'id': '4711'}, ack=True)
        while self.framesHandled != 1:
            yield task.deferLater(reactor, 0.01, lambda: None)

        yield client.disconnect()

class TransactionTestCase(AsyncClientBaseTestCase):
    frame = 'test'
    queue = '/queue/asyncTransactionTestCase'

    @defer.inlineCallbacks
    def test_transaction_commit(self):
        config = self.getConfig(StompSpec.VERSION_1_0)
        client = async.Stomp(config)
        yield client.connect(host=VIRTUALHOST)
        client.subscribe(self.queue, self._eatFrame, {StompSpec.ACK_HEADER: 'client-individual', 'id': '4711'}, ack=True)

        transaction = '4711'
        yield client.begin(transaction, receipt='%s-begin' % transaction)
        client.send(self.queue, 'test message with transaction', {StompSpec.TRANSACTION_HEADER: transaction})
        yield task.deferLater(reactor, 0.1, lambda: None)
        client.send(self.queue, 'test message without transaction')
        while self.framesHandled != 1:
            yield task.deferLater(reactor, 0.01, lambda: None)
        self.assertEquals(self.consumedFrame.body, 'test message without transaction')
        yield client.commit(transaction, receipt='%s-commit' % transaction)
        while self.framesHandled != 2:
            yield task.deferLater(reactor, 0.01, lambda: None)
        self.assertEquals(self.consumedFrame.body, 'test message with transaction')
        yield client.disconnect()

    @defer.inlineCallbacks
    def test_transaction_abort(self):
        config = self.getConfig(StompSpec.VERSION_1_0)
        client = async.Stomp(config)
        yield client.connect(host=VIRTUALHOST)
        client.subscribe(self.queue, self._eatFrame, {StompSpec.ACK_HEADER: 'client-individual', 'id': '4711'}, ack=True)

        transaction = '4711'
        yield client.begin(transaction, receipt='%s-begin' % transaction)
        client.send(self.queue, 'test message with transaction', {StompSpec.TRANSACTION_HEADER: transaction})
        yield task.deferLater(reactor, 0.1, lambda: None)
        client.send(self.queue, 'test message without transaction')
        while self.framesHandled != 1:
            yield task.deferLater(reactor, 0.01, lambda: None)
        self.assertEquals(self.consumedFrame.body, 'test message without transaction')
        yield client.abort(transaction, receipt='%s-commit' % transaction)
        yield client.disconnect(receipt='bye')
        self.assertEquals(self.framesHandled, 1)

class HeartBeatTestCase(AsyncClientBaseTestCase):
    frame = 'test'
    queue = '/queue/asyncHeartBeatTestCase'

    @defer.inlineCallbacks
    def test_heart_beat(self):
        if BROKER == 'apollo':
            print "Broker %s doesn't properly support heart-beating. Skipping test." % BROKER
            defer.returnValue(None)

        port = 61612 if (BROKER == 'activemq') else PORT
        config = self.getConfig(StompSpec.VERSION_1_1, port)
        client = async.Stomp(config)
        yield client.connect(host=VIRTUALHOST, heartBeats=(250, 250))
        disconnected = client.disconnected

        yield task.deferLater(reactor, 2.5, lambda: None)
        client.session._clientHeartBeat = 0
        try:
            yield disconnected
        except StompConnectionError:
            pass
        else:
            raise

if __name__ == '__main__':
    import sys
    from twisted.scripts import trial
    sys.argv.extend([sys.argv[0]])
    trial.run()
