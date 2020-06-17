import logging
import os
from datetime import datetime

class Logger :
    logger = None

    def myLogger(self):
        if None == self.logger:
            self.logger=logging.getLogger('nrdf')
            self.logger.setLevel(logging.DEBUG)
            log_folder = r"logs/"
            os.makedirs(os.path.dirname(log_folder), exist_ok=True)
            output_file = os.path.join(log_folder, datetime.now().strftime("%Y_%m_%d-%H_%M_%S"))
            file_handler=logging.FileHandler(output_file + '.log', mode="w", encoding=None, delay=False)
            formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
            
            stream_handler = logging.StreamHandler()
            file_handler.setFormatter(formatter)
            self.logger.addHandler(stream_handler)
            
            self.logger.propagate = False
        return self.logger