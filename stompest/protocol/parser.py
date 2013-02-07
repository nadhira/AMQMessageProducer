import collections
import cStringIO

from stompest.error import StompFrameError

from .frame import StompFrame, StompHeartBeat
from .spec import StompSpec

class StompParser(object):
    """This is a parser for a wire-level byte-stream of STOMP frames.
    
    :param version: A valid STOMP protocol version, or :obj:`None` (equivalent to the :attr:`DEFAULT_VERSION` attribute of the :class:`~.StompSpec` class).
    
    Example: 

    >>> from stompest.protocol import StompParser
    >>> messages = ['RECEIPT\\nreceipt-id:message-12345\\n\\n\\x00', 'NACK\\nsubscription:0\\nmessage-id:007\\n\\n\\x00']
    >>> parser = StompParser('1.0') # STOMP 1.0 does not support the NACK command.
    >>> for message in messages:
    ...     parser.add(message)
    ... 
    Traceback (most recent call last):
      File "<stdin>", line 2, in <module>
    stompest.error.StompFrameError: Invalid command: 'NACK'
    >>> print repr(parser.get())
    StompFrame(command='RECEIPT', headers={'receipt-id': 'message-12345'}, body='')
    >>> print parser.canRead()
    False
    >>> print parser.get()
    None
    >>> parser = StompParser('1.1')
    >>> parser.add(messages[1])
    >>> print repr(parser.get())
    StompFrame(command='NACK', headers={'message-id': '007', 'subscription': '0'}, body='')
    
    """
    def __init__(self, version=None):
        self.version = version or StompSpec.DEFAULT_VERSION
        self._parsers = {
            'heart-beat': self._parseHeartBeat,
            'command': self._parseCommand,
            'headers': self._parseHeader,
            'body': self._parseBody
        }
        self.reset()

    def canRead(self):
        """Indicates whether there are frames available.
        """
        return bool(self._frames)

    def get(self):
        """Return the next frame as a :class:`~.frame.StompFrame` object (if any), or :obj:`None` (otherwise).
        """
        if self.canRead():
            return self._frames.popleft()

    def add(self, data):
        """Add a byte-stream of wire-level data.
        
        :param data: An iterable of characters. If any character evaluates to :obj:`False`, that stream will no longer be consumed.
        """
        for character in data:
            if not character:
                return
            self.parse(character)

    def reset(self):
        """Reset internal state, including all fully or partially parsed frames.
        """
        self._frames = collections.deque()
        self._next()

    def _flush(self):
        self._buffer = cStringIO.StringIO()

    def _next(self):
        self._frame = StompFrame()
        self._length = -1
        self._read = 0
        self._transition('heart-beat')

    def _transition(self, state):
        self._flush()
        self.parse = self._parsers[state]

    def _parseHeartBeat(self, character):
        if character != StompSpec.LINE_DELIMITER:
            self._transition('command')
            self.parse(character)
            return

        if self.version != StompSpec.VERSION_1_0:
            self._frames.append(StompHeartBeat())

    def _parseCommand(self, character):
        if character != StompSpec.LINE_DELIMITER:
            self._buffer.write(character)
            return
        command = self._buffer.getvalue()
        if not command:
            return
        if command not in StompSpec.COMMANDS[self.version]:
            self._flush()
            raise StompFrameError('Invalid command: %s' % repr(command))
        self._frame.command = command
        self._transition('headers')

    def _parseHeader(self, character):
        if character != StompSpec.LINE_DELIMITER:
            self._buffer.write(character)
            return
        header = self._buffer.getvalue()
        if header:
            try:
                name, value = header.split(StompSpec.HEADER_SEPARATOR, 1)
            except ValueError:
                raise StompFrameError('No separator in header line: %s' % header)
            self._frame.headers[name] = value
            self._transition('headers')
        else:
            self._length = int(self._frame.headers.get(StompSpec.CONTENT_LENGTH_HEADER, -1))
            self._transition('body')

    def _parseBody(self, character):
        self._read += 1
        if (self._read <= self._length) or (character != StompSpec.FRAME_DELIMITER):
            self._buffer.write(character)
            return
        self._frame.body = self._buffer.getvalue()
        self._frames.append(self._frame)
        self._next()
