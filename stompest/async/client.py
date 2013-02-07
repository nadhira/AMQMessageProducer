"""The asynchronous client is based on `Twisted <http://twistedmatrix.com/>`_, a very mature and powerful asynchronous programming framework. It supports destination specific message and error handlers (with default "poison pill" error handling), concurrent message processing, graceful shutdown, and connect and disconnect timeouts.

.. seealso:: `STOMP protocol specification <http://stomp.github.com/>`_, `Twisted API documentation <http://twistedmatrix.com/documents/current/api/>`_, `Apache ActiveMQ - Stomp <http://activemq.apache.org/stomp.html>`_

Examples
--------

.. automodule:: stompest.examples
    :members:

Producer
^^^^^^^^

.. literalinclude:: ../../stompest/examples/async/producer.py

Transformer
^^^^^^^^^^^

.. literalinclude:: ../../stompest/examples/async/transformer.py

Consumer
^^^^^^^^

.. literalinclude:: ../../stompest/examples/async/consumer.py

API
---
"""
import functools
import logging
import time

from twisted.internet import defer, task, reactor

from stompest.error import StompCancelledError, StompConnectionError, StompFrameError, StompProtocolError, \
    StompAlreadyRunningError
from stompest.protocol import StompSession, StompSpec
from stompest.util import checkattr, cloneFrame

from .protocol import StompProtocolCreator
from .util import InFlightOperations, exclusive, wait

LOG_CATEGORY = __name__

connected = checkattr('_protocol')

# TODO: is it ensured that the DISCONNECT frame is the last frame we send?

class Stomp(object):
    """An asynchronous STOMP client for the Twisted framework.

    :param config: A :class:`~.StompConfig` object.
    :param receiptTimeout: When a STOMP frame was sent to the broker and a **RECEIPT** frame was requested, this is the time (in seconds) to wait for the **RECEIPT** frame to arrive. If :obj:`None`, we will wait indefinitely.
    :param heartBeatThresholds: tolerance thresholds (relative to the negotiated heart-beat periods). The default :obj:`None` is equivalent to the content of the class atrribute :attr:`DEFAULT_HEART_BEAT_THRESHOLDS`. Example: ``{'client': 0.6, 'server' 2.5}`` means that the client will send a heart-beat if it had shown no activity for 60 % of the negotiated client heart-beat period and that the client will disconnect if the server has shown no activity for 250 % of the negotiated server heart-beat period.
    
    .. note :: All API methods which may request a **RECEIPT** frame from the broker -- which is indicated by the **receipt** parameter -- will wait for the **RECEIPT** response until this client's **receiptTimeout**. Here, "wait" is to be understood in the asynchronous sense that the method's :class:`twisted.internet.defer.Deferred` result will only call back then. If **receipt** is :obj:`None`, no such header is sent, and the callback will be triggered earlier.

    .. seealso :: :class:`~.StompConfig` for how to set configuration options, :class:`~.StompSession` for session state, :mod:`.protocol.commands` for all API options which are documented here.
    """
    _protocolCreatorFactory = StompProtocolCreator

    DEFAULT_ACK_MODE = 'client-individual'
    MESSAGE_FAILED_HEADER = 'message-failed'
    DEFAULT_HEART_BEAT_THRESHOLDS = {'client': 0.8, 'server': 2.0}

    def __init__(self, config, receiptTimeout=None, heartBeatThresholds=None):
        self._config = config
        self._receiptTimeout = receiptTimeout
        self._heartBeatThresholds = heartBeatThresholds or self.DEFAULT_HEART_BEAT_THRESHOLDS

        self._session = StompSession(self._config.version, self._config.check)
        self._protocol = None
        self._protocolCreator = self._protocolCreatorFactory(self._config.uri)

        self.log = logging.getLogger(LOG_CATEGORY)

        # wait for CONNECTED frame
        self._connecting = InFlightOperations('STOMP session negotiation')
        self._disconnecting = False

        # keep track of active handlers for graceful disconnect
        self._messages = InFlightOperations('Handler for message')
        self._receipts = InFlightOperations('Waiting for receipt')

        self._handlers = {
            'MESSAGE': self._onMessage,
            'CONNECTED': self._onConnected,
            'ERROR': self._onError,
            'RECEIPT': self._onReceipt,
        }
        self._subscriptions = {}

        self._heartBeats = {}

    @property
    def disconnected(self):
        """This :class:`twisted.internet.defer.Deferred` calls back when the connection to the broker was lost. It will err back when the connection loss was unexpected or caused by another error.
        """
        return self._disconnected

    @property
    def session(self):
        """The :class:`~.StompSession` associated to this client.
        """
        return self._session

    def sendFrame(self, frame):
        """Send a raw STOMP frame.

        .. note :: If we are not connected, this method, and all other API commands for sending STOMP frames except :meth:`~.async.client.Stomp.connect`, will raise a :class:`~.StompConnectionError`. Use this command only if you have to bypass the :class:`~.StompSession` logic and you know what you're doing!
        """
        self._protocol.send(frame)
        self.session.sent()

    #
    # STOMP commands
    #
    @exclusive
    @defer.inlineCallbacks
    def connect(self, headers=None, versions=None, host=None, heartBeats=None, connectTimeout=None, connectedTimeout=None):
        """connect(headers=None, versions=None, host=None, heartBeats=None, connectTimeout=None, connectedTimeout=None)

        Establish a connection to a STOMP broker. If the wire-level connect fails, attempt a failover according to the settings in the client's :class:`~.StompConfig` object. If there are active subscriptions in the :attr:`~.async.client.Stomp.session`, replay them when the STOMP connection is established. This method returns a :class:`twisted.internet.defer.Deferred` object which calls back with :obj:`self` when the STOMP connection has been established and all subscriptions (if any) were replayed. In case of an error, it will err back with the reason of the failure.

        :param versions: The STOMP protocol versions we wish to support. The default behavior (:obj:`None`) is the same as for the :func:`~.commands.connect` function of the commands API, but the highest supported version will be the one you specified in the :class:`~.StompConfig` object. The version which is valid for the connection about to be initiated will be stored in the :attr:`~.async.client.Stomp.session`.
        :param connectTimeout: This is the time (in seconds) to wait for the wire-level connection to be established. If :obj:`None`, we will wait indefinitely.
        :param connectedTimeout: This is the time (in seconds) to wait for the STOMP connection to be established (that is, the broker's **CONNECTED** frame to arrive). If :obj:`None`, we will wait indefinitely.

        .. note :: Only one connect attempt may be pending at a time. Any other attempt will result in a :class:`~.StompAlreadyRunningError`.

        .. seealso :: The :mod:`.protocol.failover` and :mod:`~.protocol.session` modules for the details of subscription replay and failover transport.
        """
        frame = self.session.connect(self._config.login, self._config.passcode, headers, versions, host, heartBeats)

        try:
            self._protocol
        except:
            pass
        else:
            raise StompConnectionError('Already connected')

        try:
            self._protocol = yield self._protocolCreator.connect(connectTimeout, self.session.version, self._onFrame, self._onConnectionLost)
        except Exception as e:
            self.log.error('Endpoint connect failed')
            raise

        self._disconnected = defer.Deferred()
        self._disconnectReason = None

        try:
            with self._connecting(None, self.log) as connected:
                self.sendFrame(frame)
                yield wait(connected, connectedTimeout, StompCancelledError('STOMP broker did not answer on time [timeout=%s]' % connectedTimeout))
        except Exception as e:
            self.log.error('Could not establish STOMP session. Disconnecting ...')
            yield self.disconnect(failure=e)

        self._replay()

        defer.returnValue(self)

    @connected
    def disconnect(self, receipt=None, failure=None, timeout=None):
        """disconnect(self, receipt=None, failure=None, timeout=None)
        
        Send a **DISCONNECT** frame and terminate the STOMP connection. This method returns a :class:`twisted.internet.defer.Deferred` object which calls back with :obj:`None` when the STOMP connection has been closed. In case of a failure, it will err back with the failure reason.

        :param failure: A disconnect reason (a :class:`Exception`) to err back. Example: ``versions=['1.0', '1.1']``
        :param timeout: This is the time (in seconds) to wait for a graceful disconnect, thas is, for pending message handlers to complete. If receipt is :obj:`None`, we will wait indefinitely.

        .. note :: The :attr:`~.async.client.Stomp.session`'s active subscriptions will be cleared if no failure has been passed to this method. This allows you to replay the subscriptions upon reconnect. If you do not wish to do so, you have to clear the subscriptions yourself by calling the :meth:`~.StompSession.close` method of the :attr:`~.async.client.Stomp.session`. Only one disconnect attempt may be pending at a time. Any other attempt will result in a :class:`~.StompAlreadyRunningError`. The result of any (user-requested or not) disconnect event is available via the :attr:`disconnected` property.
        """
        if self._disconnecting:
            raise StompAlreadyRunningError('Already disconnecting')
        self._disconnecting = True
        self._disconnect(receipt, failure, timeout)
        return self.disconnected

    @defer.inlineCallbacks
    def _disconnect(self, receipt, failure, timeout):
        if failure:
            self._disconnectReason = failure

        self.log.info('Disconnecting ...%s' % ('' if (not failure) else  ('[reason=%s]' % failure)))
        protocol = self._protocol
        try:
            # notify that we are ready to disconnect after outstanding messages are ack'ed
            if self._messages:
                self.log.info('Waiting for outstanding message handlers to finish ... [timeout=%s]' % timeout)
                try:
                    yield task.cooperate(iter([wait(handler, timeout, StompCancelledError('Going down to disconnect now')) for handler in self._messages.values()])).whenDone()
                except StompCancelledError as e:
                    self._disconnectReason = StompCancelledError('Handlers did not finish in time.')
                else:
                    self.log.info('All handlers complete. Resuming disconnect ...')

            if self.session.state == self.session.CONNECTED:
                frame = self.session.disconnect(receipt)
                try:
                    self.sendFrame(frame)
                except Exception as e:
                    self._disconnectReason = StompConnectionError('Could not send %s. [%s]' % (frame.info(), e))

                try:
                    yield self._waitForReceipt(receipt)
                except StompCancelledError:
                    self._disconnectReason = StompCancelledError('Receipt for disconnect command did not arrive on time.')

            protocol.loseConnection()

        except Exception as e:
            self._disconnectReason = e

    @connected
    @defer.inlineCallbacks
    def send(self, destination, body='', headers=None, receipt=None):
        """send(destination, body='', headers=None, receipt=None)

        Send a **SEND** frame.
        """
        self.sendFrame(self.session.send(destination, body, headers, receipt))
        yield self._waitForReceipt(receipt)

    @connected
    @defer.inlineCallbacks
    def ack(self, frame, receipt=None):
        """ack(frame, receipt=None)

        Send an **ACK** frame for a received **MESSAGE** frame.
        """
        self.sendFrame(self.session.ack(frame, receipt))
        yield self._waitForReceipt(receipt)

    @connected
    @defer.inlineCallbacks
    def nack(self, frame, receipt=None):
        """nack(frame, receipt=None)

        Send a **NACK** frame for a received **MESSAGE** frame.
        """
        self.sendFrame(self.session.nack(frame, receipt))
        yield self._waitForReceipt(receipt)

    @connected
    @defer.inlineCallbacks
    def begin(self, transaction=None, receipt=None):
        """begin(transaction=None, receipt=None)

        Send a **BEGIN** frame to begin a STOMP transaction.
        """
        frame = self.session.begin(transaction, receipt)
        self.sendFrame(frame)
        yield self._waitForReceipt(receipt)

    @connected
    @defer.inlineCallbacks
    def abort(self, transaction=None, receipt=None):
        """abort(transaction=None, receipt=None)

        Send an **ABORT** frame to abort a STOMP transaction.
        """
        frame = self.session.abort(transaction, receipt)
        self.sendFrame(frame)
        yield self._waitForReceipt(receipt)

    @connected
    @defer.inlineCallbacks
    def commit(self, transaction=None, receipt=None):
        """commit(transaction=None, receipt=None)

        Send a **COMMIT** frame to commit a STOMP transaction.
        """
        frame = self.session.commit(transaction, receipt)
        self.sendFrame(frame)
        yield self._waitForReceipt(receipt)

    @connected
    @defer.inlineCallbacks
    def subscribe(self, destination, handler, headers=None, receipt=None, ack=True, errorDestination=None, onMessageFailed=None):
        """subscribe(destination, handler, headers=None, receipt=None, ack=True, errorDestination=None, onMessageFailed=None)

        Send a **SUBSCRIBE** frame to subscribe to a STOMP destination. This method returns a :class:`twisted.internet.defer.Deferred` object which will fire with a token when a possibly requested **RECEIPT** frame has arrived. The callback value is a token which is used internally to match incoming **MESSAGE** frames and must be kept if you wish to :meth:`~.async.client.Stomp.unsubscribe` later.

        :param handler: A callable :obj:`f(client, frame)` which accepts this client and the received :class:`~.StompFrame`.
        :param ack: Check this option if you wish the client to automatically ack MESSAGE frames when the were handled (successfully or not).
        :param errorDestination: If a frame was not handled successfully, forward a copy of the offending frame to this destination. Example: ``errorDestination='/queue/back-to-square-one'``
        :param onMessageFailed: You can specify a custom error handler which must be a callable with signature :obj:`f(self, failure, frame, errorDestination)`. Note that a non-trivial choice of this error handler overrides the default behavior (forward frame to error destination and ack it).

        .. note :: As opposed to the behavior of stompest 1.x, the client will not disconnect when a message could not be handled. Rather, a disconnect will only be triggered in a "panic" situation when also the error handler failed. The automatic disconnect was partly a substitute for the missing NACK command in STOMP 1.0. If you wish to automatically disconnect, you have to implement the **onMessageFailed** hook.
        """
        if not callable(handler):
            raise ValueError('Cannot subscribe (handler is missing): %s' % handler)
        frame, token = self.session.subscribe(destination, headers, receipt, {'handler': handler, 'errorDestination': errorDestination, 'onMessageFailed': onMessageFailed})
        ack = ack and (frame.headers.setdefault(StompSpec.ACK_HEADER, self.DEFAULT_ACK_MODE) in StompSpec.CLIENT_ACK_MODES)
        self._subscriptions[token] = {'destination': destination, 'handler': self._createHandler(handler), 'ack': ack, 'errorDestination': errorDestination, 'onMessageFailed': onMessageFailed}
        self.sendFrame(frame)
        yield self._waitForReceipt(receipt)
        defer.returnValue(token)

    @connected
    @defer.inlineCallbacks
    def unsubscribe(self, token, receipt=None):
        """unsubscribe(token, receipt=None)

        Send an **UNSUBSCRIBE** frame to terminate an existing subscription.

        :param token: The result of the :meth:`~.async.client.Stomp.subscribe` command which initiated the subscription in question.
        """
        frame = self.session.unsubscribe(token, receipt)
        try:
            self._subscriptions.pop(token)
        except:
            self.log.warning('Cannot unsubscribe (subscription id unknown): %s=%s' % token)
            raise
        self.sendFrame(frame)
        yield self._waitForReceipt(receipt)

    #
    # callbacks for received STOMP frames
    #
    def _onFrame(self, frame):
        self.session.received()
        if not frame:
            return
        try:
            handler = self._handlers[frame.command]
        except KeyError:
            raise StompFrameError('Unknown STOMP command: %s' % repr(frame))
        handler(frame)

    def _onConnected(self, frame):
        self.session.connected(frame)
        self.log.info('Connected to stomp broker [session=%s]' % self.session.id)
        self._connecting[None].callback(None)
        self._beats()

    def _onError(self, frame):
        if self._connecting:
            self._connecting[None].errback(StompProtocolError('While trying to connect, received %s' % frame.info()))
            return

        #Workaround for AMQ < 5.2
        if 'Unexpected ACK received for message-id' in frame.headers.get('message', ''):
            self.log.debug('AMQ brokers < 5.2 do not support client-individual mode')
        else:
            self.disconnect(failure=StompProtocolError('Received %s' % frame.info()))

    @defer.inlineCallbacks
    def _onMessage(self, frame):
        headers = frame.headers
        messageId = headers[StompSpec.MESSAGE_ID_HEADER]

        if self._disconnecting:
            self.log.info('[%s] Ignoring message (disconnecting)' % messageId)
            try:
                self.nack(frame)
            except StompProtocolError:
                pass
            defer.returnValue(None)

        try:
            token = self.session.message(frame)
            subscription = self._subscriptions[token]
        except:
            self.log.error('[%s] Ignoring message (no handler found): %s' % (messageId, frame.info()))
            defer.returnValue(None)

        with self._messages(messageId, self.log):
            try:
                yield subscription['handler'](self, frame)
                if subscription['ack']:
                    self.ack(frame)
            except Exception as e:
                try:
                    self._onMessageFailed(e, frame, subscription)
                except Exception as e:
                    if not self._disconnecting:
                        self.disconnect(failure=e)
                finally:
                    if subscription['ack']:
                        self.ack(frame)

    def _onReceipt(self, frame):
        receipt = self.session.receipt(frame)
        self._receipts[receipt].callback(None)

    #
    # hook for MESSAGE frame error handling
    #
    def _onMessageFailed(self, failure, frame, subscription):
        onMessageFailed = subscription['onMessageFailed'] or Stomp.sendToErrorDestination
        onMessageFailed(self, failure, frame, subscription['errorDestination'])

    def sendToErrorDestination(self, failure, frame, errorDestination):
        """sendToErrorDestination(failure, frame, errorDestination)

        This is the default error handler for failed **MESSAGE** handlers: forward the offending frame to the error destination (if given) and ack the frame. As opposed to earlier versions, It may be used as a building block for custom error handlers.

        .. seealso :: The **onMessageFailed** argument of the :meth:`~.async.client.Stomp.subscribe` method.
        """
        if not errorDestination:
            return
        errorFrame = cloneFrame(frame, persistent=True)
        errorFrame.headers.setdefault(self.MESSAGE_FAILED_HEADER, str(failure))
        self.send(errorDestination, errorFrame.body, errorFrame.headers)

    #
    # private properties
    #
    @property
    def _protocol(self):
        protocol = self.__protocol
        if not protocol:
            raise StompConnectionError('Not connected')
        return protocol

    @_protocol.setter
    def _protocol(self, protocol):
        self.__protocol = protocol

    @property
    def _disconnectReason(self):
        return self.__disconnectReason

    @_disconnectReason.setter
    def _disconnectReason(self, reason):
        if reason:
            self.log.error(str(reason))
            reason = self._disconnectReason or reason # existing reason wins
        self.__disconnectReason = reason

    #
    # private helpers
    #

    def _beat(self, which):
        try:
            self._heartBeats.pop(which).cancel()
        except:
            pass
        remaining = self._beatRemaining(which)
        if remaining < 0:
            return
        if not remaining:
            if which == 'client':
                self.sendFrame(self.session.beat())
                remaining = self._beatRemaining(which)
            else:
                self.disconnect(failure=StompConnectionError('Server heart-beat timeout'))
                return
        self._heartBeats[which] = reactor.callLater(remaining, self._beat, which) #@UndefinedVariable

    def _beatRemaining(self, which):
        heartBeat = {'client': self.session.clientHeartBeat, 'server': self.session.serverHeartBeat}[which]
        if not heartBeat:
            return -1
        last = {'client': self.session.lastSent, 'server': self.session.lastReceived}[which]
        elapsed = time.time() - last
        return max((self._heartBeatThresholds[which] * heartBeat / 1000.0) - elapsed, 0)

    def _beats(self):
        for which in ('client', 'server'):
            self._beat(which)

    def _createHandler(self, handler):
        @functools.wraps(handler)
        def _handler(_, result):
            return handler(self, result)
        return _handler

    def _onConnectionLost(self, reason):
        self._protocol = None
        self.log.info('Disconnected: %s' % reason.getErrorMessage())
        if not self._disconnecting:
            self._disconnectReason = StompConnectionError('Unexpected connection loss [%s]' % reason.getErrorMessage())
        self.session.close(flush=not self._disconnectReason)
        self._beats()
        for operations in (self._connecting, self._messages, self._receipts):
            for waiting in operations.values():
                if not waiting.called:
                    waiting.errback(StompCancelledError('In-flight operation cancelled (connection lost)'))
                    waiting.addErrback(lambda _: None)
        if self._disconnectReason:
            self.log.debug('Calling disconnected deferred errback: %s' % self._disconnectReason)
            self._disconnected.errback(self._disconnectReason)
        else:
            #self.log.debug('Calling disconnected deferred callback')
            self._disconnected.callback(None)
        self._disconnecting = False
        self._disconnectReason = None
        self._disconnected = None

    def _replay(self):
        for (destination, headers, receipt, context) in self.session.replay():
            self.log.info('Replaying subscription: %s' % headers)
            self.subscribe(destination, headers=headers, receipt=receipt, **context)

    @defer.inlineCallbacks
    def _waitForReceipt(self, receipt):
        if receipt is None:
            defer.returnValue(None)
        with self._receipts(receipt, self.log) as receiptArrived:
            timeout = self._receiptTimeout
            yield wait(receiptArrived, timeout, StompCancelledError('Receipt did not arrive on time: %s [timeout=%s]' % (receipt, timeout)))
