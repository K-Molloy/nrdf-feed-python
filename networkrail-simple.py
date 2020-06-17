import os
import sys
import glob
import json
from time import sleep
from datetime import datetime

import stomp
from pymongo import MongoClient

from logger import Logger
from listener import Listener
from installation.installation import Installation
from installation.schedule import Schedule



def nrdf_feed():
    # Create logger instance
    logger = Logger()
    logrep = logger.myLogger()

    # Load Config from file
    _config_location = r'config.json'
    with open(_config_location, 'r') as f:
        _config = json.load(f)   
    logrep.info('Successfully loaded config file : /%s' % _config_location)

    # Create Mongo Connection
    mongodb = MongoClient(_config['mongo']['connection-string'])
    db = mongodb[_config['mongo']['db-name']]
    logrep.info('MongoDB Connection Established [%s:%s]' % (_config['mongo']['connection-string'],_config['mongo']['db-name']))
            
    # TODO: Installation logic goes here
    if (_config['installation']['fresh']==True):
        logrep.info('Fresh Installation = True | Starting Installation Procedure')
        ins = Installation(db,logrep,_config)
        #ins.importCORPUS()
        #ins.importSMART()
        #ins.importReference(None)
        ins.importFullSchedule()
    else:
        logrep.info('Fresh Installation = False | Skipping')
        
        
    # Create Stomp Logic
    mq = stomp.Connection(host_and_ports=[(_config['stomp-connection']['host-url'], _config['stomp-connection']['host-port'])],
                keepalive=True,
                vhost=_config['stomp-connection']['host-url'],
                heartbeats=(10000, 5000))

    # Create Listener Object
    mq.set_listener('', Listener(mq,logrep))

    # Connect
    mq.connect( username=_config['stomp-connection']['username'],
                passcode=_config['stomp-connection']['password'],
                wait=True)
    logrep.info('Stomp Connection Established [%s:%i]' % (_config['stomp-connection']['host-url'], _config['stomp-connection']['host-port']))

    # Subscribe to basic topic
    mq.subscribe("/topic/TRAIN_MVT_ALL_TOC", 'test-mvt', ack='client-individual')

    logrep.info('Stomp Topic Subscribed: %s','TRAIN_MVT_ALL_TOC')

    # Disconnect
    #wait=input("Press Enter to continue...")
    mq.disconnect()

    logrep.info('Stomp Disconnected')


    #while mq.is_connected():
    #    sleep(1)




# Main 
nrdf_feed()
    