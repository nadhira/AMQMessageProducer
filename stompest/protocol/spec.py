class StompSpec(object):
    """This class hosts all constants related to the STOMP protocol specification in its various versions. There really isn't much to document, but you are invited to take a look at all available constants in the source code. Wait a minute ... one attribute is particularly noteworthy, name :attr:`DEFAULT_VERSION` --- which currently is :obj:`'1.0'` (but this may change in upcoming stompest releases, so you're advised to always explicitly define which STOMP protocol version you are going to use).
    
    .. seealso :: Specification of STOMP protocols `1.0 <http://stomp.github.com//stomp-specification-1.0.html>`_ and `1.1 <http://stomp.github.com//stomp-specification-1.1.html>`_, your favorite broker's documentation for additional STOMP headers.
    """
    # specification of the STOMP protocol: http://stomp.github.com//index.html
    VERSION_1_0, VERSION_1_1 = '1.0', '1.1'
    VERSIONS = [VERSION_1_0, VERSION_1_1]
    DEFAULT_VERSION = VERSION_1_0

    ABORT = 'ABORT'
    ACK = 'ACK'
    BEGIN = 'BEGIN'
    COMMIT = 'COMMIT'
    CONNECT = 'CONNECT'
    DISCONNECT = 'DISCONNECT'
    NACK = 'NACK'
    SEND = 'SEND'
    STOMP = 'STOMP'
    SUBSCRIBE = 'SUBSCRIBE'
    UNSUBSCRIBE = 'UNSUBSCRIBE'

    CLIENT_COMMANDS = {
        '1.0': set([
            ABORT, ACK, BEGIN, COMMIT, CONNECT, DISCONNECT,
            SEND, SUBSCRIBE, UNSUBSCRIBE
        ]),
        '1.1': set([
            ABORT, ACK, BEGIN, COMMIT, CONNECT, DISCONNECT,
            NACK, SEND, STOMP, SUBSCRIBE, UNSUBSCRIBE
        ])
    }

    CONNECTED = 'CONNECTED'
    ERROR = 'ERROR'
    MESSAGE = 'MESSAGE'
    RECEIPT = 'RECEIPT'

    SERVER_COMMANDS = {
        '1.0': set([CONNECTED, ERROR, MESSAGE, RECEIPT]),
        '1.1': set([CONNECTED, ERROR, MESSAGE, RECEIPT])
    }

    COMMANDS = dict(CLIENT_COMMANDS)
    for (version, commands) in SERVER_COMMANDS.iteritems():
        COMMANDS.setdefault(version, set()).update(commands)

    LINE_DELIMITER = '\n'
    FRAME_DELIMITER = '\x00'
    HEADER_SEPARATOR = ':'

    ACCEPT_VERSION_HEADER = 'accept-version'
    ACK_HEADER = 'ack'
    CONTENT_LENGTH_HEADER = 'content-length'
    CONTENT_TYPE_HEADER = 'content-type'
    DESTINATION_HEADER = 'destination'
    HEART_BEAT_HEADER = 'heart-beat'
    HOST_HEADER = 'host'
    ID_HEADER = 'id'
    LOGIN_HEADER = 'login'
    MESSAGE_ID_HEADER = 'message-id'
    PASSCODE_HEADER = 'passcode'
    RECEIPT_HEADER = 'receipt'
    RECEIPT_ID_HEADER = 'receipt-id'
    SESSION_HEADER = 'session'
    SERVER_HEADER = 'server'
    SUBSCRIPTION_HEADER = 'subscription'
    TRANSACTION_HEADER = 'transaction'
    VERSION_HEADER = 'version'

    ACK_AUTO = 'auto'
    ACK_CLIENT = 'client'
    ACK_CLIENT_INDIVIDUAL = 'client-individual'
    CLIENT_ACK_MODES = set([ACK_CLIENT, ACK_CLIENT_INDIVIDUAL])

    HEART_BEAT_SEPARATOR = ','
