# Python Test Cases for NRDF Components

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

def testFullScheduleRead():

    # Create logger instance
    logger = Logger()
    logg = logger.myLogger()

    inst = {
        "fresh" : True,
        "bulk-size" : 10000
    }

    sch = Schedule(None, logg, inst)

    logg.info('Reading Schedule File')
    state0 = sch.readTimetableVersion()
    logg.info('Completed version.json')
    state1 = sch.readAssociation()
    logg.info('Completed association.json')
    state2 = sch.readTiploc()
    logg.info('Completed tiploc.json')
    state3 = sch.readSchedule()
    logg.info('Completed schedule.json')

def 

testFullScheduleRead()