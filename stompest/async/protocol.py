import logging

from twisted.internet import defer, reactor, task
from twisted.internet.protocol import Factory, Protocol

from stompest.protocol import StompFailoverTransport, StompParser

from .util import endpointFactory

LOG_CATEGORY = __name__

class StompProtocol(Protocol):
    #
    # twisted.internet.Protocol interface overrides
    #
    def connectionLost(self, reason):
        try:
            self._onConnectionLost(reason)
        finally:
            Protocol.connectionLost(self, reason)

    def dataReceived(self, data):
        #self.log.debug('Received data: %s' % repr(data))
        self._parser.add(data)
        while True:
            frame = self._parser.get()
            if frame is None:
                break
            if self.log.isEnabledFor(logging.DEBUG):
                self.log.debug('Received %s' % frame.info())
            try:
                self._onFrame(frame)
            except Exception as e:
                self.log.error('Unhandled error in frame handler: %s' % e)

    def __init__(self, version, onFrame, onConnectionLost):
        self._onFrame = onFrame
        self._onConnectionLost = onConnectionLost

        # leave the used logger public in case the user wants to override it
        self.log = logging.getLogger(LOG_CATEGORY)

        self._parser = StompParser(version)

    #
    # user interface
    #
    def send(self, frame):
        if self.log.isEnabledFor(logging.DEBUG):
            self.log.debug('Sending %s' % frame.info())
        self.transport.write(str(frame))

    def loseConnection(self):
        self.transport.loseConnection()

class StompFactory(Factory):
    protocol = StompProtocol

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def buildProtocol(self, _):
        protocol = self.protocol(*self.args, **self.kwargs)
        protocol.factory = self
        return protocol

class StompProtocolCreator(object):
    protocolFactory = StompFactory
    failoverFactory = StompFailoverTransport

    @classmethod
    def endpointFactory(cls, broker, timeout=None):
        return endpointFactory(broker, timeout)

    def __init__(self, uri):
        self._failover = self.failoverFactory(uri)
        self.log = logging.getLogger(LOG_CATEGORY)

    @defer.inlineCallbacks
    def connect(self, timeout, *args, **kwargs):
        for (broker, delay) in self._failover:
            yield self._sleep(delay)
            endpoint = self.endpointFactory(broker, timeout)
            self.log.info('Connecting to %(host)s:%(port)s ...' % broker)
            try:
                protocol = yield endpoint.connect(self.protocolFactory(*args, **kwargs))
            except Exception as e:
                self.log.warning('%s [%s]' % ('Could not connect to %(host)s:%(port)d' % broker, e))
            else:
                defer.returnValue(protocol)

    def _sleep(self, delay):
        if not delay:
            return
        self.log.info('Delaying connect attempt for %d ms' % int(delay * 1000))
        return task.deferLater(reactor, delay, lambda: None)
