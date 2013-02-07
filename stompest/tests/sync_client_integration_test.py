import logging
import unittest

from stompest.config import StompConfig
from stompest.error import StompConnectionError
from stompest.protocol import commands, StompFrame, StompSpec
from stompest.sync import Stomp
import time

logging.basicConfig(level=logging.DEBUG)
LOG_CATEGORY = __name__

from . import HOST, PORT, VERSION, LOGIN, PASSCODE, VIRTUALHOST, BROKER

class SimpleStompIntegrationTest(unittest.TestCase):
    DESTINATION = '/queue/stompUnitTest'
    TIMEOUT = 0.1
    log = logging.getLogger(LOG_CATEGORY)

    def getConfig(self, version, port=PORT):
        return StompConfig('tcp://%s:%s' % (HOST, port), login=LOGIN, passcode=PASSCODE, version=version)

    def setUp(self):
        config = self.getConfig(StompSpec.VERSION_1_0)
        client = Stomp(config)
        client.connect(host=VIRTUALHOST)
        client.subscribe(self.DESTINATION, {StompSpec.ACK_HEADER: 'auto'})
        client.subscribe(self.DESTINATION, {StompSpec.ID_HEADER: 'bla', StompSpec.ACK_HEADER: 'auto'})
        while client.canRead(self.TIMEOUT):
            frame = client.receiveFrame()
            self.log.debug('Dequeued old %s' % frame.info())
        client.disconnect()

    def test_1_integration(self):
        config = self.getConfig(StompSpec.VERSION_1_0)
        client = Stomp(config)
        client.connect(host=VIRTUALHOST)

        client.send(self.DESTINATION, 'test message 1')
        client.send(self.DESTINATION, 'test message 2')
        self.assertFalse(client.canRead(self.TIMEOUT))
        client.subscribe(self.DESTINATION, {StompSpec.ACK_HEADER: 'client-individual'})
        self.assertTrue(client.canRead(self.TIMEOUT))
        client.ack(client.receiveFrame())
        self.assertTrue(client.canRead(self.TIMEOUT))
        client.ack(client.receiveFrame())
        self.assertFalse(client.canRead(self.TIMEOUT))

    def test_2_transaction(self):
        config = self.getConfig(StompSpec.VERSION_1_0)
        client = Stomp(config)
        client.connect(host=VIRTUALHOST)
        client.subscribe(self.DESTINATION, {StompSpec.ACK_HEADER: 'client-individual'})
        self.assertFalse(client.canRead(self.TIMEOUT))

        with client.transaction(4711) as transaction:
            self.assertEquals(transaction, '4711')
            client.send(self.DESTINATION, 'test message', {StompSpec.TRANSACTION_HEADER: transaction})
            self.assertFalse(client.canRead(0))
        self.assertTrue(client.canRead(self.TIMEOUT))
        frame = client.receiveFrame()
        self.assertEquals(frame.body, 'test message')
        client.ack(frame)

        with client.transaction(4713, receipt='4712') as transaction:
            self.assertEquals(transaction, '4713')
            self.assertEquals(client.receiveFrame(), StompFrame(StompSpec.RECEIPT, {'receipt-id': '4712-begin'}))
            client.send(self.DESTINATION, 'test message', {StompSpec.TRANSACTION_HEADER: transaction})
            client.send(self.DESTINATION, 'test message without transaction')
            self.assertTrue(client.canRead(self.TIMEOUT))
            frame = client.receiveFrame()
            self.assertEquals(frame.body, 'test message without transaction')
            client.ack(frame)
            self.assertFalse(client.canRead(0))
        frames = [client.receiveFrame() for _ in xrange(2)]
        frames = list(sorted(frames, key=lambda f: f.command))
        frame = frames[0]
        client.ack(frame)
        self.assertEquals(frame.body, 'test message')
        frame = frames[1]
        self.assertEquals(frame, StompFrame(StompSpec.RECEIPT, {'receipt-id': '4712-commit'}))

        try:
            with client.transaction(4714) as transaction:
                self.assertEquals(transaction, '4714')
                client.send(self.DESTINATION, 'test message', {StompSpec.TRANSACTION_HEADER: transaction})
                raise RuntimeError('poof')
        except RuntimeError as e:
            self.assertEquals(str(e), 'poof')
        else:
            raise
        self.assertFalse(client.canRead(self.TIMEOUT))

        client.disconnect()

    def test_3_timeout(self):
        timeout = 0.2
        client = Stomp(StompConfig(uri='failover:(tcp://localhost:61610,tcp://localhost:61613)?startupMaxReconnectAttempts=1,randomize=false', login=LOGIN, passcode=PASSCODE, version=StompSpec.VERSION_1_0))
        client.connect(host=VIRTUALHOST, connectTimeout=timeout)
        client.disconnect()

        client = Stomp(StompConfig(uri='failover:(tcp://localhost:61610,tcp://localhost:61611)?startupMaxReconnectAttempts=1,backOffMultiplier=3', login=LOGIN, passcode=PASSCODE, version=StompSpec.VERSION_1_0))
        self.assertRaises(StompConnectionError, client.connect, host=VIRTUALHOST, connectTimeout=timeout)

        client = Stomp(StompConfig(uri='failover:(tcp://localhost:61610,tcp://localhost:61613)?randomize=false', login=LOGIN, passcode=PASSCODE, version=StompSpec.VERSION_1_0)) # default is startupMaxReconnectAttempts = 0
        self.assertRaises(StompConnectionError, client.connect, host=VIRTUALHOST, connectTimeout=timeout)

    def test_3_socket_failure_and_replay(self):
        client = Stomp(self.getConfig(StompSpec.VERSION_1_0))
        client.connect(host=VIRTUALHOST)
        headers = {StompSpec.ACK_HEADER: 'client-individual'}
        token = client.subscribe(self.DESTINATION, headers)
        client.sendFrame(StompFrame('DISCONNECT')) # DISCONNECT frame is out-of-band, as far as the session is concerned -> unexpected disconnect
        self.assertRaises(StompConnectionError, client.receiveFrame)
        client.connect(host=VIRTUALHOST)
        client.send(self.DESTINATION, 'test message 1')
        client.ack(client.receiveFrame())
        client.unsubscribe(token)
        headers = {'id': 'bla', StompSpec.ACK_HEADER: 'client-individual'}
        client.subscribe(self.DESTINATION, headers)
        headers[StompSpec.DESTINATION_HEADER] = self.DESTINATION
        client.sendFrame(StompFrame('DISCONNECT')) # DISCONNECT frame is out-of-band, as far as the session is concerned -> unexpected disconnect
        self.assertRaises(StompConnectionError, client.receiveFrame)
        client.connect(host=VIRTUALHOST)
        client.send(self.DESTINATION, 'test message 2')
        client.ack(client.receiveFrame())
        client.unsubscribe(('id', 'bla'))
        client.disconnect()

    def test_4_integration_stomp_1_1(self):
        if StompSpec.VERSION_1_1 not in commands.versions(VERSION):
            print 'This broker does not support STOMP protocol version 1.1'
            return

        client = Stomp(self.getConfig(StompSpec.VERSION_1_1))
        client.connect(host=VIRTUALHOST)

        client.send(self.DESTINATION, 'test message 1')
        client.send(self.DESTINATION, 'test message 2')
        self.assertFalse(client.canRead(self.TIMEOUT))
        token = client.subscribe(self.DESTINATION, {StompSpec.ID_HEADER: 4711, StompSpec.ACK_HEADER: 'client-individual'})
        self.assertTrue(client.canRead(self.TIMEOUT))
        client.ack(client.receiveFrame())
        self.assertTrue(client.canRead(self.TIMEOUT))
        client.ack(client.receiveFrame())
        self.assertFalse(client.canRead(self.TIMEOUT))
        client.unsubscribe(token)
        client.send(self.DESTINATION, 'test message 3', receipt='4711')
        self.assertTrue(client.canRead(self.TIMEOUT))
        self.assertEquals(client.receiveFrame(), StompFrame(StompSpec.RECEIPT, {'receipt-id': '4711'}))
        self.assertFalse(client.canRead(self.TIMEOUT))
        client.subscribe(self.DESTINATION, {StompSpec.ID_HEADER: 4711, StompSpec.ACK_HEADER: 'client-individual'})
        self.assertTrue(client.canRead(self.TIMEOUT))
        client.ack(client.receiveFrame())
        self.assertFalse(client.canRead(self.TIMEOUT))
        client.disconnect(receipt='4712')
        self.assertEquals(client.receiveFrame(), StompFrame(StompSpec.RECEIPT, {'receipt-id': '4712'}))
        self.assertRaises(StompConnectionError, client.receiveFrame)
        client.connect(host=VIRTUALHOST)
        client.disconnect(receipt='4711')
        self.assertEquals(client.receiveFrame(), StompFrame(StompSpec.RECEIPT, {'receipt-id': '4711'}))
        client.close()
        self.assertRaises(StompConnectionError, client.canRead, 0)

    def test_5_integration_stomp_1_1_heartbeat(self):
        if BROKER == 'apollo':
            print "Broker %s doesn't properly support heart-beating. Skipping test." % BROKER
            return

        if StompSpec.VERSION_1_1 not in commands.versions(VERSION):
            print 'This broker does not support STOMP protocol version 1.1'
            return

        port = 61612 if (BROKER == 'activemq') else PORT # stomp+nio on 61613 does not work properly, so use stomp on 61612
        client = Stomp(self.getConfig(StompSpec.VERSION_1_1, port))
        self.assertEquals(client.lastReceived, None)
        self.assertEquals(client.lastSent, None)

        heartBeatPeriod = 100
        client.connect(host=VIRTUALHOST, heartBeats=(heartBeatPeriod, heartBeatPeriod))
        self.assertTrue((time.time() - client.lastReceived) < 0.1)
        if not (client.serverHeartBeat and client.clientHeartBeat):
            print 'broker does not support heart-beating. disconnecting ...'
            client.disconnect()
            client.close()
            return

        serverHeartBeatInSeconds = client.serverHeartBeat / 1000.0
        clientHeartBeatInSeconds = client.clientHeartBeat / 1000.0

        start = time.time()
        while (time.time() - start) < (2.5 * max(serverHeartBeatInSeconds, clientHeartBeatInSeconds)):
            time.sleep(0.5 * min(serverHeartBeatInSeconds, clientHeartBeatInSeconds))
            client.canRead(0)
            self.assertTrue((time.time() - client.lastReceived) < (1.5 * serverHeartBeatInSeconds))
            if (time.time() - client.lastSent) > (0.5 * clientHeartBeatInSeconds):
                client.beat()
                self.assertTrue((time.time() - client.lastSent) < 0.1)

        start = time.time()
        try:
            while not client.canRead(0.5 * clientHeartBeatInSeconds):
                pass
        except StompConnectionError:
            self.assertTrue((time.time() - start) < (3.0 * clientHeartBeatInSeconds))
            self.assertTrue((time.time() - client.lastReceived) < (1.5 * serverHeartBeatInSeconds))
            self.assertTrue((time.time() - client.lastSent) > clientHeartBeatInSeconds)
        else:
            raise
        client.close()

if __name__ == '__main__':
    unittest.main()
