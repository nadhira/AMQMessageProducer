import unittest

from stompest.error import StompProtocolError
from stompest.protocol import commands, StompSpec, StompFrame

class CommandsTest(unittest.TestCase):
    def test_connect(self):
        self.assertEquals(commands.connect(), StompFrame(StompSpec.CONNECT))
        self.assertEquals(commands.connect(login='hi'), StompFrame(StompSpec.CONNECT, headers={StompSpec.LOGIN_HEADER: 'hi'}))
        self.assertEquals(commands.connect(passcode='there'), StompFrame(StompSpec.CONNECT, headers={StompSpec.PASSCODE_HEADER: 'there'}))
        self.assertEquals(commands.connect('hi', 'there'), StompFrame(StompSpec.CONNECT, headers={StompSpec.LOGIN_HEADER: 'hi', StompSpec.PASSCODE_HEADER: 'there'}))
        self.assertEquals(commands.connect('hi', 'there', {'4711': '0815'}), StompFrame(StompSpec.CONNECT, headers={StompSpec.LOGIN_HEADER: 'hi', StompSpec.PASSCODE_HEADER: 'there', '4711': '0815'}))

        self.assertEquals(commands.connect('hi', 'there', versions=[StompSpec.VERSION_1_0]), StompFrame(StompSpec.CONNECT, headers={StompSpec.LOGIN_HEADER: 'hi', StompSpec.PASSCODE_HEADER: 'there'}))

        frame = commands.connect(versions=[StompSpec.VERSION_1_0, StompSpec.VERSION_1_1])
        frame.headers.pop(StompSpec.HOST_HEADER)
        self.assertEquals(frame, StompFrame(StompSpec.CONNECT, headers={StompSpec.ACCEPT_VERSION_HEADER: '1.0,1.1'}))

        frame = commands.connect(versions=[StompSpec.VERSION_1_1])
        frame.headers.pop(StompSpec.HOST_HEADER)
        self.assertEquals(frame, StompFrame(StompSpec.CONNECT, headers={StompSpec.ACCEPT_VERSION_HEADER: StompSpec.VERSION_1_1}))

        frame = commands.connect(versions=[StompSpec.VERSION_1_1], login='hi', passcode='there', host='earth')
        self.assertEquals(frame, StompFrame(StompSpec.CONNECT, headers={'accept-version': '1.1', 'login': 'hi', 'passcode': 'there', 'host': 'earth'}))

        frame = commands.connect(versions=[StompSpec.VERSION_1_1], login='hi', passcode='there', host='earth', heartBeats=(1, 2))
        self.assertEquals(frame, StompFrame(StompSpec.CONNECT, headers={'accept-version': '1.1', 'login': 'hi', 'passcode': 'there', 'host': 'earth', 'heart-beat': '1,2'}))

        frame = commands.connect('hi', 'there', None, [StompSpec.VERSION_1_1], 'earth', (1, 2))
        self.assertEquals(frame, StompFrame(StompSpec.CONNECT, headers={'accept-version': '1.1', 'login': 'hi', 'passcode': 'there', 'host': 'earth', 'heart-beat': '1,2'}))

        for heartBeats in [('bla', 'bla'), (-1, 0), (0, -1), (-1, -1), 1, -1, 'bla']:
            self.assertRaises(StompProtocolError, commands.connect, versions=[StompSpec.VERSION_1_1], login='hi', passcode='there', host='earth', heartBeats=heartBeats)

        frame = commands.connect(versions=[StompSpec.VERSION_1_1], host='earth', headers={'4711': '0815', StompSpec.HEART_BEAT_HEADER: '1,2'})
        self.assertEquals(frame, StompFrame(StompSpec.CONNECT, headers={'accept-version': '1.1', '4711': '0815', 'host': 'earth', 'heart-beat': '1,2'}))
        stompFrame = commands.stomp('hi', 'there', {'4711': '0815'}, [StompSpec.VERSION_1_1], 'earth')
        self.assertEquals(stompFrame.command, StompSpec.STOMP)
        self.assertEquals(stompFrame.headers, commands.connect('hi', 'there', {'4711': '0815'}, [StompSpec.VERSION_1_1], 'earth').headers)
        self.assertRaises(StompProtocolError, commands.stomp, 'hi', 'there', {'4711': '0815'}, [StompSpec.VERSION_1_0], 'earth')
        self.assertRaises(StompProtocolError, commands.stomp, 'hi', 'there', {'4711': '0815'}, None, 'earth')

    def test_disconnect(self):
        self.assertEquals(commands.disconnect(), StompFrame(StompSpec.DISCONNECT))
        self.assertEquals(commands.disconnect(receipt='4711'), StompFrame(StompSpec.DISCONNECT, {StompSpec.RECEIPT_HEADER: '4711'}))
        self.assertRaises(StompProtocolError, commands.disconnect, receipt=4711)

    def test_connected(self):
        self.assertEquals(commands.connected(StompFrame(StompSpec.CONNECTED, {StompSpec.SESSION_HEADER: 'hi'})), ('1.0', None, 'hi', (0, 0)))
        self.assertEquals(commands.connected(StompFrame(StompSpec.CONNECTED, {})), ('1.0', None, None, (0, 0)))
        self.assertEquals(commands.connected(StompFrame(StompSpec.CONNECTED, {StompSpec.SESSION_HEADER: 'hi'}), versions=['1.0']), ('1.0', None, 'hi', (0, 0)))
        self.assertEquals(commands.connected(StompFrame(StompSpec.CONNECTED, {StompSpec.SESSION_HEADER: 'hi'}), versions=['1.0', '1.1']), ('1.0', None, 'hi', (0, 0)))
        self.assertRaises(StompProtocolError, commands.connected, StompFrame(StompSpec.CONNECTED, {StompSpec.SESSION_HEADER: 'hi'}), versions=['1.1'])
        self.assertRaises(StompProtocolError, commands.connected, StompFrame(StompSpec.MESSAGE, {}), versions=['1.0'])
        self.assertRaises(StompProtocolError, commands.connected, StompFrame(StompSpec.MESSAGE, {}), versions=['1.1'])
        self.assertEquals(commands.connected(StompFrame(StompSpec.CONNECTED, {StompSpec.SESSION_HEADER: 'hi', StompSpec.VERSION_HEADER: '1.1'}), versions=['1.1']), ('1.1', None, 'hi', (0, 0)))
        self.assertEquals(commands.connected(StompFrame(StompSpec.CONNECTED, {StompSpec.SERVER_HEADER: 'moon', StompSpec.VERSION_HEADER: '1.1'}), versions=['1.1']), ('1.1', 'moon', None, (0, 0)))
        self.assertEquals(commands.connected(StompFrame(StompSpec.CONNECTED, {StompSpec.SESSION_HEADER: 'hi', StompSpec.VERSION_HEADER: '1.1'}), versions=['1.0', '1.1']), ('1.1', None, 'hi', (0, 0)))
        self.assertEquals(commands.connected(StompFrame(StompSpec.CONNECTED, {StompSpec.SERVER_HEADER: 'moon', StompSpec.VERSION_HEADER: '1.1'}), versions=['1.0', '1.1']), ('1.1', 'moon', None, (0, 0)))

        self.assertEquals(commands.connected(StompFrame(StompSpec.CONNECTED, {StompSpec.SESSION_HEADER: 'hi', StompSpec.VERSION_HEADER: '1.1', StompSpec.HEART_BEAT_HEADER: '1,2'}), versions=['1.1']), ('1.1', None, 'hi', (1, 2)))
        self.assertEquals(commands.connected(StompFrame(StompSpec.CONNECTED, {StompSpec.SESSION_HEADER: 'hi', StompSpec.VERSION_HEADER: '1.0', StompSpec.HEART_BEAT_HEADER: '1,2'}), versions=['1.0']), ('1.0', None, 'hi', (0, 0)))

        for heartBeats in ('-1,0', '0,-1', '-1,-1', '2', ',', ',2', '2,', None):
            self.assertRaises(StompProtocolError, commands.connected, StompFrame(StompSpec.CONNECTED, {StompSpec.SESSION_HEADER: 'hi', StompSpec.VERSION_HEADER: '1.1', StompSpec.HEART_BEAT_HEADER: heartBeats}), versions=['1.1'])

    def test_ack(self):
        self.assertEquals(commands.ack(StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi'})), StompFrame(command='ACK', headers={'message-id': 'hi'}))
        self.assertEquals(commands.ack(StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi', StompSpec.SUBSCRIPTION_HEADER: 'there'})), StompFrame(command='ACK', headers={'message-id': 'hi', 'subscription': 'there'}))
        self.assertEquals(commands.ack(StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi', StompSpec.SUBSCRIPTION_HEADER: 'there', StompSpec.TRANSACTION_HEADER: 'man'}), ['man', 'woman']), StompFrame(command='ACK', headers={'message-id': 'hi', 'subscription': 'there', 'transaction': 'man'}))
        self.assertEquals(commands.ack(StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi', StompSpec.SUBSCRIPTION_HEADER: 'there', StompSpec.TRANSACTION_HEADER: 'man'}), transactions=['woman']), StompFrame(command='ACK', headers={'message-id': 'hi', 'subscription': 'there'}))
        self.assertEquals(commands.ack(StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi', StompSpec.SUBSCRIPTION_HEADER: 'there', StompSpec.TRANSACTION_HEADER: 'man'})), StompFrame(command='ACK', headers={'message-id': 'hi', 'subscription': 'there'}))
        self.assertEquals(commands.ack(StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi'}), receipt='4711'), StompFrame(command='ACK', headers={'message-id': 'hi', StompSpec.RECEIPT_HEADER: '4711'}))
        self.assertEquals(commands.ack(StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi', StompSpec.SUBSCRIPTION_HEADER: 'there', '4711': '0815', StompSpec.TRANSACTION_HEADER: 'man'})), StompFrame(command='ACK', headers={'message-id': 'hi', 'subscription': 'there'}))
        self.assertEquals(commands.ack(StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi', StompSpec.SUBSCRIPTION_HEADER: 'there', '4711': '0815', StompSpec.TRANSACTION_HEADER: 'man'}), ['man'], '4711'), StompFrame(command='ACK', headers={'message-id': 'hi', 'subscription': 'there', 'transaction': 'man', 'receipt': '4711'}))
        self.assertEquals(commands.ack(StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi', StompSpec.SUBSCRIPTION_HEADER: 'there', StompSpec.TRANSACTION_HEADER: 'man'}), version='1.1'), StompFrame(command='ACK', headers={'message-id': 'hi', 'subscription': 'there'}))
        self.assertEquals(commands.ack(StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi', StompSpec.SUBSCRIPTION_HEADER: 'there', '4711': '0815', StompSpec.TRANSACTION_HEADER: 'man'}), transactions=set(['man', 'woman']), version='1.1'), StompFrame(command='ACK', headers={'message-id': 'hi', 'subscription': 'there', 'transaction': 'man'}))
        self.assertRaises(StompProtocolError, commands.ack, StompFrame(StompSpec.CONNECTED, {StompSpec.MESSAGE_ID_HEADER: 'hi'}), version='1.0')
        self.assertRaises(StompProtocolError, commands.ack, StompFrame(StompSpec.CONNECTED, {StompSpec.MESSAGE_ID_HEADER: 'hi'}), version='1.1')
        self.assertRaises(StompProtocolError, commands.ack, StompFrame(StompSpec.MESSAGE, {StompSpec.SUBSCRIPTION_HEADER: 'hi'}))
        self.assertRaises(StompProtocolError, commands.ack, StompFrame(StompSpec.MESSAGE, {StompSpec.SUBSCRIPTION_HEADER: 'hi'}), version='1.1')
        self.assertRaises(StompProtocolError, commands.ack, StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi'}), version='1.1')

    def test_nack(self):
        self.assertEquals(commands.nack(StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi', StompSpec.SUBSCRIPTION_HEADER: 'there'}), version='1.1'), StompFrame(command='NACK', headers={'message-id': 'hi', 'subscription': 'there'}))
        self.assertEquals(commands.nack(StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi', StompSpec.SUBSCRIPTION_HEADER: 'there'}), receipt='4711', version='1.1'), StompFrame(command='NACK', headers={'message-id': 'hi', 'subscription': 'there', StompSpec.RECEIPT_HEADER: '4711'}))
        self.assertEquals(commands.nack(StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi', StompSpec.SUBSCRIPTION_HEADER: 'there', StompSpec.TRANSACTION_HEADER: 'man'}), version='1.1'), StompFrame(command='NACK', headers={'message-id': 'hi', 'subscription': 'there'}))
        self.assertEquals(commands.nack(StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi', StompSpec.SUBSCRIPTION_HEADER: 'there', StompSpec.TRANSACTION_HEADER: 'man'}), transactions=['woman'], version='1.1'), StompFrame(command='NACK', headers={'message-id': 'hi', 'subscription': 'there'}))
        self.assertEquals(commands.nack(StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi', StompSpec.SUBSCRIPTION_HEADER: 'there', StompSpec.TRANSACTION_HEADER: 'man'}), ['man', 'woman'], None, '1.1'), StompFrame(command='NACK', headers={'message-id': 'hi', 'subscription': 'there', 'transaction': 'man'}))
        self.assertEquals(commands.nack(StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi', StompSpec.SUBSCRIPTION_HEADER: 'there', '4711': '0815'}), version='1.1'), StompFrame(command='NACK', headers={'message-id': 'hi', 'subscription': 'there'}))
        self.assertRaises(StompProtocolError, commands.nack, StompFrame(StompSpec.CONNECTED, {}), version='1.1')
        self.assertRaises(StompProtocolError, commands.nack, StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi', StompSpec.SUBSCRIPTION_HEADER: 'there'}), version='1.0')
        self.assertRaises(StompProtocolError, commands.nack, StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi', StompSpec.SUBSCRIPTION_HEADER: 'there', StompSpec.TRANSACTION_HEADER: 'man'}), version='1.0')
        self.assertRaises(StompProtocolError, commands.nack, StompFrame(StompSpec.MESSAGE, {StompSpec.SUBSCRIPTION_HEADER: 'hi'}), version='1.1')
        self.assertRaises(StompProtocolError, commands.nack, StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi'}), version='1.1')

if __name__ == '__main__':
    unittest.main()
