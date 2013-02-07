import binascii
import unittest

from stompest.error import StompFrameError
from stompest.protocol import commands
from stompest.protocol.frame import StompFrame, StompHeartBeat
from stompest.protocol.parser import StompParser
from stompest.protocol.spec import StompSpec

class StompParserTest(unittest.TestCase):
    def _generate_bytes(self, stream):
        for byte in stream:
            yield byte
        while True:
            yield ''

    def test_frameParse_succeeds(self):
        message = {
            'command': 'SEND',
            'headers': {'foo': 'bar', 'hello ': 'there-world with space ', 'empty-value':'', '':'empty-header', StompSpec.DESTINATION_HEADER: '/queue/blah'},
            'body': 'some stuff\nand more'
        }
        frame = StompFrame(**message)
        parser = StompParser()

        parser.add(str(frame))
        self.assertEqual(parser.get(), frame)
        self.assertEqual(parser.get(), None)
        parser = StompParser()

    def test_reset_succeeds(self):
        message = {
            'command': 'SEND',
            'headers': {'foo': 'bar', 'hello ': 'there-world with space ', 'empty-value':'', '':'empty-header', StompSpec.DESTINATION_HEADER: '/queue/blah'},
            'body': 'some stuff\nand more'
        }
        frame = StompFrame(**message)
        parser = StompParser()

        parser.add(str(frame))
        parser.reset()
        self.assertEqual(parser.get(), None)
        parser.add(str(frame)[:20])
        self.assertEqual(parser.get(), None)

    def test_frame_without_header_or_body_succeeds(self):
        parser = StompParser()
        parser.add(str(commands.disconnect()))
        self.assertEqual(parser.get(), commands.disconnect())

    def test_frames_with_optional_newlines_succeeds(self):
        parser = StompParser()
        disconnect = commands.disconnect()
        frame = '\n%s\n' % disconnect
        parser.add(2 * frame)
        for _ in xrange(2):
            self.assertEqual(parser.get(), disconnect)
        self.assertEqual(parser.get(), None)

    def test_frames_with_heart_beats_succeeds(self):
        parser = StompParser(version=StompSpec.VERSION_1_1)
        disconnect = commands.disconnect()
        frame = '\n%s\n' % disconnect
        parser.add(2 * frame)
        frames = []
        while parser.canRead():
            frames.append(parser.get())
        self.assertEquals(frames, [StompHeartBeat(), disconnect, StompHeartBeat(), StompHeartBeat(), disconnect, StompHeartBeat()])

        #self.assert frames   
        #StompFrame(command='DISCONNECT', headers={}, body=''), StompFrame(command='DISCONNECT', headers={}, body='')]

        #self.assertEqual(parser.get(), commands.disconnect())
        self.assertEqual(parser.get(), None)

    def test_getMessage_returns_None_if_not_done(self):
        parser = StompParser()
        self.assertEqual(None, parser.get())
        parser.add('CONNECT')
        self.assertEqual(None, parser.get())

    def test_processLine_throws_FrameError_on_invalid_command(self):
        parser = StompParser()

        self.assertRaises(StompFrameError, lambda: parser.add('HELLO\n'))
        self.assertFalse(parser.canRead())
        parser.add('DISCONNECT\n\n\x00')
        self.assertEquals(StompFrame('DISCONNECT'), parser.get())
        self.assertFalse(parser.canRead())

    def test_processLine_throws_FrameError_on_header_line_missing_separator(self):
        parser = StompParser()
        parser.add('SEND\n')
        self.assertRaises(StompFrameError, lambda: parser.add('no separator\n'))

    def test_no_newline(self):
        headers = {'x': 'y'}
        body = 'testing 1 2 3'
        frameBytes = str(StompFrame('MESSAGE', headers, body))
        self.assertTrue(frameBytes.endswith('\x00'))
        parser = StompParser()
        parser.add(self._generate_bytes(frameBytes))
        frame = parser.get()
        self.assertEquals('MESSAGE', frame.command)
        self.assertEquals(headers, frame.headers)
        self.assertEquals(body, frame.body)
        self.assertEquals(parser.get(), None)

    def test_binary_body(self):
        body = binascii.a2b_hex('f0000a09')
        headers = {'content-length': str(len(body))}
        frameBytes = str(StompFrame('MESSAGE', headers, body))
        self.assertTrue(frameBytes.endswith('\x00'))
        parser = StompParser()
        parser.add(frameBytes)
        frame = parser.get()
        self.assertEquals('MESSAGE', frame.command)
        self.assertEquals(headers, frame.headers)
        self.assertEquals(body, frame.body)

        self.assertEquals(parser.get(), None)

    def test_receiveFrame_multiple_frames_per_read(self):
        body1 = 'boo'
        body2 = 'hoo'
        headers = {'x': 'y'}
        frameBytes = str(StompFrame('MESSAGE', headers, body1)) + str(StompFrame('MESSAGE', headers, body2))
        parser = StompParser()
        parser.add(frameBytes)

        frame = parser.get()
        self.assertEquals('MESSAGE', frame.command)
        self.assertEquals(headers, frame.headers)
        self.assertEquals(body1, frame.body)

        frame = parser.get()
        self.assertEquals('MESSAGE', frame.command)
        self.assertEquals(headers, frame.headers)
        self.assertEquals(body2, frame.body)

        self.assertEquals(parser.get(), None)

if __name__ == '__main__':
    unittest.main()
