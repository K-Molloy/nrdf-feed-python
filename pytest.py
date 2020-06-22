# Python Test Cases for NRDF Components

import os
import sys
import glob
import json
from time import sleep
from datetime import datetime

import stomp
from pymongo import MongoClient, InsertOne

from logger import Logger
from listener.listener import Listener
from installation.installation import Installation
from installation.schedule import Schedule

def testFullScheduleRead():

    # Create logger instance
    logger = Logger()
    logg = logger.myLogger()

    mongodb = MongoClient("mongodb://localhost:27017/nrdf_test_db")
    db = mongodb["nrdf_test_db"]

    inst = {
        "location": "/home/kieran/nrdf-project/nrdf-feed-python/",
        "full" : True,
        "update": False,
        "bulk-operations-size" : 1000,
        "schedule-file-size" : 25000,
        "standard-bulk-size": 10000
    }

    sch = Schedule(db, logg, inst)

    sch.importSchedule()
    
    return True

    logg.info('Reading Schedule File')
    state0 = sch.readTimetableVersion()
    logg.info('Completed version.json')
    state1 = sch.readAssociation()
    logg.info('Completed association.json')
    state2 = sch.readTiploc()
    logg.info('Completed tiploc.json')
    state3 = sch.readSchedule()
    logg.info('Completed schedule.json')


testFullScheduleRead()