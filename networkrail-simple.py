import os
import sys
import glob
import json
from time import sleep
from datetime import datetime

import stomp
from pymongo import MongoClient

from logger import Logger
from listener.listener import Listener
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
    logrep.info('Successfully loaded config file : %s' % _config_location)

    # Load Selected Mongo Environment
    if _config['mongo-type'] == "test":
        mongoConfig = _config['mongo-test']
    else:
        mongoConfig = _config['mongo-live']

    # Network Rail Rules state security token must be present
    logrep.info('Process Name : {}'.format(_config['process-id']))
    logrep.info('Network Rail Security Token : {}'.format(_config['security-token']))

    # Create Mongo Connection
    mongodb = MongoClient(mongoConfig['connection-string'],
                        username=mongoConfig['username'],
                        password=mongoConfig['password'],
                        authSource='admin',
                        authMechanism='SCRAM-SHA-256')

    db = mongodb[mongoConfig['db-name']]
    logrep.info('MongoDB Connection Established [%s:%s]' % (mongoConfig['connection-string'],mongoConfig['db-name']))
            
    # Full Installation Process - Potentially needs some tweaking
    if (_config['installation']['full']==True):
        logrep.info('Installation = True | Starting Installation Procedure')
        ins = Installation(db,logrep,_config)
        ins.importCORPUS()
        ins.importSMART()
        ins.importReference(_config['installation']['geography'])
        ins.downloadFullSchedule()
        ins.importFullSchedule()
        
        
    # Create Stomp Logic
    mq = stomp.Connection(host_and_ports=[(_config['stomp-connection']['host-url'], _config['stomp-connection']['host-port'])],
                keepalive=True,
                vhost=_config['stomp-connection']['host-url'],
                heartbeats=(20000, 10000))

    # Create Listener Object
    mq.set_listener('', Listener(mq,logrep,db, _config))

    wait = input('')
    mq.disconnect()

    while mq.is_connected():
        sleep(1)


# Main 
nrdf_feed()
    