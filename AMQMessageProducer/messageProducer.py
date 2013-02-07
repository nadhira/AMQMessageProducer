'''
Created on Jan 29, 2013

@author: AKINAVCI
'''
from stompest.config import StompConfig
from stompest.sync import Stomp
import logging

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

# TODO: Read from config file....
CONFIG = StompConfig("tcp://localhost:61613")
#QUEUE = "pods2jbpm"
    
def send_message(messageBody, destination=None, queueName=None):
    
    client = None
    if destination != None:
        client = Stomp(StompConfig(destination))
    else:
        client = Stomp(StompConfig("tcp://localhost:61613"))
    
    QUEUE = None
    if queueName != None:
        QUEUE = queueName
    else:
        QUEUE = "pods2jbpm"
        
    #client = Stomp(CONFIG)
    client.connect()
    
    body = messageBody
    
    client.send(QUEUE, body)
    
    client.disconnect()
