import json
import os
from datetime import datetime

class Listener:
    message_count = 0
    output_folder = r"data/"

    def __init__(self, mq, logg):
        self._mq = mq
        self.output_path = os.path.join(self.output_folder, "%s.json" % datetime.now().strftime("%Y_%m_%d.%H_%M_%S"))
        self.logg = logg
        
    def on_message(self, headers, message):
        self.message_count += 1
        self.logg.info('Message Recieved | %i' % self.message_count)
        with open(self.output_path, 'w',encoding='utf-8') as outfile:
            json.dump(message, outfile)
            
        self._mq.ack(id=headers['message-id'], subscription=headers['subscription'])

    def on_error(self,headers,message):
        self.logg.error('Error at "%s"' % message)

    def on_disconnected(self):
        self.logg.info('Listener Disconnected')