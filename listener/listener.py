import json
import os
from datetime import datetime
from time import sleep
from numpy import random


from listener.trust import TRUST
from listener.td import TD
from listener.vstp import VSTP
from listener.rtppm import RTPPM
from listener.tsr import TSR

class Listener:


    # Message Counters
    # 1 - TRUST
    # 2 - TD
    # 3 - VSTP
    # 4 - RTPPM
    # 5 - TSR
    # 6 - Unknown
    message_counters = [0,0,0,0,0,0]

    # Exponential Backoff Timer
    timeout = 0



    def __init__(self, mq, logg, db, config):
        """Initialise Listener Object

        Args:
            mq ([Stomp.py Connection]): [Listener Object]
            logg ([Logging.py Object]): [Logging Object]
            db ([MongoDB Connection]): [MongoDB Connection Object]
            config ([Configuration])
        """

        # Save MQ Object
        self.mq = mq

        # Save Logging Object
        self.logg = logg

        # Save MongoDB Connection Object
        self.mongodb = db

        # configuration files
        self.processName = config['process-id']
        self.confStomp = config['stomp-connection']
        self.confProcessing = config['processing']
        self.confSubscriptions = config['subscriptions']


        # Call Connection
        self.connectAndSubscribe(self.confStomp, self.confProcessing, self.confSubscriptions)

        # TODO : Potentially only create those required as in config.json
        self.trustParser = TRUST(db,logg)

        self.tdParser = TD(db,logg)

        self.vstpParser = VSTP(db,logg)

        self.rtppmParser = RTPPM(db,logg)

        self.tsrParser = TSR(db,logg)
        
        
    def on_message(self, headers, message):

        self.printCounters()


        # Determine Sending System & Call Correct Function 
        self.determineSenderSystem(headers,message)

        # Send Ack
        self.mq.ack(id=headers['message-id'], subscription=headers['subscription'])



    def on_error(self,headers,message):
        self.logg.error('Error at "%s"' % message)

    def on_disconnected(self):
        self.logg.info('Listener Disconnected')


    def on_heartbeat_timeout(self):
        self.logg.error('Heartbeat Timeout | Attempting Reconnect in {}'.format(self.timeout))

        # Exponential Backoff
        sleep((2 ** self.timeout) + (random.randint(0, 1000) / 1000))

        # Maximum Backoff Time = 32 seconds
        if self.timeout < 32:
            self.timeout = self.timeout*2

        self.connectAndSubscribe(self.confStomp, self.confProcessing, self.confSubscriptions)




    def determineSenderSystem(self,headers,message):
        """Determine Sending System Based on Header Content

        Args:
            headers ([Dict]): Headers from STOMP Message
            message ([Dict]): Message from STOMP Message

        Returns:
            [type]: [description]
        """

        if "TRUST" in headers['subscription']:
            # Increment Message Counters
            self.message_counters[0] += 1
            self.printTRUST()
            self.trustParser.on_message(message)

        elif "TD" in headers['subscription']:
            # Increment Message Counters
            self.message_counters[1] += 1
            self.tdParser.on_message(message)

        elif "VSTP" in headers['subscription']:
            # Increment Message Counters
            self.message_counters[2] += 1
            self.vstpParser.on_message(message)

        elif "RTPPM" in headers['subscription']:
            # Increment Message Counters
            self.message_counters[3] += 1
            self.rtppmParser.on_message(message)

        elif "TSR" in headers['subscription']:
            # Increment Message Counters
            self.message_counters[4] += 1
            self.tsrParser.on_message(message)

        else:
            # Increment Message Counters
            self.message_counters[5] += 1
            self.logg.error('MSG Unrecognised')

    def connectAndSubscribe(self, stomp, processing, subscriptions):

        # Connect
        try:
            self.mq.connect(username=stomp['username'],
                            passcode=stomp['password'],

                            wait=True)
            self.logg.info('Stomp Connection Established [%s:%i]' % (stomp['host-url'], stomp['host-port']))
        except:
            self.logg.error('Stomp Connection Error')

        # Subscription to Topics
        if processing['trust']:
            # TRUST -> Train Movements

            topic = '/topic/' + subscriptions['mvt-channel']
            topicId = self.processName + 'nrdf-TRUST'

            self.mq.subscribe(topic, topicId, ack='client-individual')
            self.logg.info('Stomp Topic Subscribed: {}'.format(topic))

        if processing['td']:
            # TD -> Train Describer
            
            topic = '/topic/' + subscriptions['td-channel']
            topicId = self.processName + 'nrdf-TD'

            self.mq.subscribe(topic, topicId, ack='client-individual')
            self.logg.info('Stomp Topic Subscribed: {}'.format(topic))

        if processing['vstp']:
            # VSTP -> Very Short Term Plan VSTP_ALL

            topic = '/topic/VSTP_ALL'
            topicId = self.processName + 'nrdf-VSTP'

            self.mq.subscribe(topic, topicId, ack='client-individual')
            self.logg.info('Stomp Topic Subscribed: {}'.format(topic))

        if processing['rtppm']:
            # RTPMM -> Real Time Performance Measure

            topic = '/topic/RTPPM_ALL'
            topicId = self.processName + 'nrdf-RTPPM'

            self.mq.subscribe(topic, topicId, ack='client-individual')
            self.logg.info('Stomp Topic Subscribed: {}'.format(topic))

        if processing['tsr']:
            # TSR -> Temporary Speed Restrictions

            topic = '/topic/' + subscriptions['tsr-channel']
            topicId = self.processName + 'nrdf-TSR'

            self.mq.subscribe(topic, topicId, ack='client-individual')
            self.logg.info('Stomp Topic Subscribed: {}'.format(topic))

    def printCounters(self):
        self.logg.info("Messages Recieved : {0} | {1}".format(sum(self.message_counters),self.message_counters))
        None

    def printTRUST(self):
        #self.logg.info("Messages Recieved : {0} \n{1}".format(sum(self.trustParser.getCounters),self.trustParser.getCounters))
        None

    def printTD(self):
        self.logg.info('Not Yet Implemented  :)')
        #self.logg.info("Messages Recieved : {0} \n{1}".format(sum(self.tdParser.getCounters),self.tdParser.getCounters))
        




