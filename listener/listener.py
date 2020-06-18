import json
import os
from datetime import datetime

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

    # TEMP : Save Messages to File 
    output_folder = r"data/"

    def __init__(self, mq, logg, db):
        """Initialise Listener Object

        Args:
            mq ([Stomp.py Connection]): [Listener Object]
            logg ([Logging.py Object]): [Logging Object]
            db ([MongoDB Connection]): [MongoDB Connection Object]
        """

        # TEMP : Save messages to File
        self.output_path = os.path.join(self.output_folder, "%s.json" % datetime.now().strftime("%Y_%m_%d.%H_%M_%S"))

        # Save MQ Object
        self._mq = mq

        # Save Logging Object
        self.logg = logg

        # Save MongoDB Connection Object
        self.mongodb = db

        # Create Message Parsing Objects
        self.logg.info('Creating Message Parsing Objects')

        # Pass DB into all

        # TODO : Potentially only create those required as in config.json
        self.trustParser = TRUST(db)

        self.tdParser = TD(db)

        self.vstpParser = VSTP(db)

        self.rtppmParser = RTPPM(db)

        self.tsrParser = TSR(db)

        
    def on_message(self, headers, message):

        self.logg.debug('Message Recieved')


        # Determine Sending System & Call Correct Function 
        try:
            self.determineSenderSystem(headers,message)
        except:
            self.logg.error('Problem Importing Message')

        # TEMP : Save Messages to File
        #with open(self.output_path, 'w') as outfile:
            #outfile.write(message)
            
        self._mq.ack(id=headers['message-id'], subscription=headers['subscription'])

        if sum(self.message_counters)==25:
            self.printCounters()


    def on_error(self,headers,message):
        self.logg.error('Error at "%s"' % message)

    def on_disconnected(self):
        self.logg.info('Listener Disconnected')


    def on_heartbeat_timeout(self):
        self.logg.error('Heartbeat Timeout')


    def determineSenderSystem(self,headers,message):
        """Determine Sending System Based on Header Content

        Args:
            headers ([Dict]): Headers from STOMP Message
            message ([Dict]): Message from STOMP Message

        Returns:
            [type]: [description]
        """

        if headers['subscription']=="TRUST":
            # Increment Message Counters
            self.message_counters[0] += 1
            self.trustParser.on_message(message)

        elif headers['subscription']=="TD":
            # Increment Message Counters
            self.message_counters[1] += 1
            self.tdParser.on_message(message)

        elif headers['subscription']=="VSTP":
            # Increment Message Counters
            self.message_counters[2] += 1
            self.vstpParser.on_message(message)

        elif headers['subscription']=="RTPPM":
            # Increment Message Counters
            self.message_counters[3] += 1
            self.rtppmParser.on_message(message)

        elif headers['subscription']=="TSR":
            # Increment Message Counters
            self.message_counters[4] += 1
            self.tsrParser.on_message(message)

        else:
            # Increment Message Counters
            self.message_counters[5] += 1
            return "Unknown"

    def printCounters(self):
        self.logg.info("Messages Recieved : {0} \n{1}".format(sum(self.message_counters),self.message_counters))




