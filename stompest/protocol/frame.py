from .spec import StompSpec

class StompFrame(object):
    """This object represents a STOMP frame which consists of a STOMP :attr:`command`, :attr:`headers`, and a message :attr:`body`. Its string representation (via :meth:`__str__`) renders the wire-level STOMP frame."""
    INFO_LENGTH = 20

    def __init__(self, command='', headers=None, body=''):
        self.command = str(command)
        self.headers = {} if (headers is None) else dict(map(str, item) for item in headers.iteritems())
        self.body = str(body)

    def __eq__(self, other):
        return all(getattr(self, key) == getattr(other, key) for key in ('command', 'headers', 'body'))

    def __iter__(self):
        return self.__dict__.iteritems()

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, ', '.join("%s=%s" % (key, repr(self.__dict__[key])) for key in ('command', 'headers', 'body')))

    def __str__(self):
        headers = ''.join('%s:%s%s' % (key, value, StompSpec.LINE_DELIMITER) for (key, value) in self.headers.iteritems())
        return StompSpec.LINE_DELIMITER.join([self.command, headers, '%s%s' % (self.body, StompSpec.FRAME_DELIMITER)])

    def info(self):
        """Produce a log-friendly representation of the frame (show only non-trivial content, and truncate the message to INFO_LENGTH characters.)"""
        headers = self.headers and 'headers=%s' % self.headers
        body = self.body[:self.INFO_LENGTH]
        if body not in self.body:
            body = '%s...' % body
        body = body and ('body=%s' % repr(body))
        info = ', '.join(i for i in (headers, body) if i)
        return '%s frame%s' % (self.command, info and (' [%s]' % info))

class StompHeartBeat(object):
    """This object represents a STOMP heart-beat. Its string representation (via :meth:`__str__`) renders the wire-level STOMP heart-beat."""
    __slots__ = ()

    def __eq__(self, other):
        return isinstance(other, StompHeartBeat)

    def __nonzero__(self):
        return False

    def __repr__(self):
        return '%s()' % self.__class__.__name__

    def __str__(self):
        return StompSpec.LINE_DELIMITER

    def info(self):
        return 'heart-beat'
