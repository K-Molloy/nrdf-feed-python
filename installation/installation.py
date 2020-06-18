import os
import json
import requests
import gzip
import re
import pandas as pd
from installation.schedule import Schedule

class Installation:
    
    def __init__(self,db,logrep,_config):
        self.mongodb = db
        self.logg = logrep
        self.inst = _config['installation']
        self.auth = (_config['stomp-connection']['username'],_config['stomp-connection']['password'])

        data_folder = r"data/sch"
        os.makedirs(os.path.dirname(data_folder), exist_ok=True)
        
        
    def importSMART(self):
        self.logg.info('Starting SMART Import')
        # Download SMART File
        if ~os.path.exists(r'data/smart.json.gz'):
            r = requests.get('http://datafeeds.networkrail.co.uk/ntrod/SupportingFileAuthenticate?type=SMART',
                            auth=(self.auth[0], self.auth[1]))
            self.logg.info('SMART HTTP Request [%i] | Downloading File' % r.status_code)
            open(r'data/smart.json.gz', 'wb').write(r.content)
        else:
            self.logg.info('SMART File Detected, skipping download')
            
        # Import SMART File to MongoDB
        
        totalImportedDocuments = 0
        
        self.logg.info('Reading SMART File')
        with gzip.open(r'data/smart.json.gz', "rt", encoding="utf-8") as f:
            data = json.load(f)
            
        self.logg.info('Importing SMART into MongoDB')
        for entry in data['BERTHDATA']:
            db_confirm = self.mongodb['smart'].insert_one(entry) 
            totalImportedDocuments += 1
            if ((totalImportedDocuments%5000) == 0):
                self.logg.info('SMART Progress | {:.0%}'.format(totalImportedDocuments/4e4))  
        self.logg.info('Completed SMART Import Successfully')  

    def importReference(self,filename):
    
        self.logg.info('Starting Reference Import')
        
        if filename == None:
            self.logg.info('Filename Not Set | Beginning File Search')
            fileDir = os.path.dirname(os.path.abspath(__file__))
            regex = re.compile('^Geography\_\d{8}\_\B(to)_\d{8}\_\B(from)\_\d{8}(.txt.gz)')

            for root, dirs, files in os.walk(fileDir):
                for file in files:
                    if regex.match(file):
                        self.logg.info('Compatible File Found')
                        filename = os.path.join(os.path.abspath(root), file)
                        
        # Reference File is .tsv
        self.logg.info('Starting Reading Reference File')
        my_cols = ["TYPE","A", "B", "C", "D", "E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z"]

        # Initialise docType Counters
        totalImportedDocuments = [0,0,0,0,0,0,0,0]
        bulkCounter = 0

        # Open file & read into pd.DataFrame
        with gzip.open(filename, "rt", encoding="cp1252") as f:
            df = pd.read_csv(f, sep="\t",names=my_cols)
            
            self.logg.info('Importing Reference into MongoDB')

            bulkStore = []

            for row in df.itertuples(index=False):
                # Reset out_file
                out_file = None

                if row[0] == 'PIF':
                    # REFTYPE : Document Specification
                    out_file = {
                        "docType": "PIF",
                        "fileVersion": row[1],
                        "sourceSystem": row[2],
                        "TOCid": row[3],
                        "timetableStartDate": row[4],
                        "timetableEndDate": row[5],
                        "cycleType": row[6],
                        "cycleStage": row[7],
                        "creationDate": row[8],
                        "fileSequenceNumber": row[9],
                    }
                    totalImportedDocuments[0] += 1
                elif row[0] == "REF":
                    # REFTYPE : Reference Code
                    out_file = {
                        "docType": "REF",
                        "actionCode": row[1],
                        "codeType": row[2],
                        "description": row[3]
                    }
                    totalImportedDocuments[1] += 1
                elif row[0] == "TLD":
                    # REFTYPE : Timing Load
                    out_file = {
                        "docType": "TLD",
                        "actionCode": row[1],
                        "tractionType": row[2],
                        "trailingLoad": row[3],
                        "speed": row[4],
                        "raGauge": row[5],
                        "description": row[6],
                        "ITPSPowerType": row[7],
                        "ITPSLoad": row[8],
                        "limitingSpeed": row[9],
                    }
                    totalImportedDocuments[2] += 1
                elif row[0] == "LOC":
                    # REFTYPE : Geographical Data
                    out_file = {
                        "docType": "LOC",
                        "actionCode": row[1],
                        "TIPLOC": row[2],
                        "locationName": row[3],
                        "startDate": row[4],
                        "endDate": row[5],
                        "northing": row[6],
                        "easting": row[7],
                        "timingType": row[8],
                        "zone": row[9],
                        "STANOX": row[10],
                        "offNetwork": row[11],
                        "forceLPB": row[12],
                    }
                    totalImportedDocuments[3] += 1
                elif row[0] == "PLT":
                    # REFTYPE : Platform
                    out_file = {
                        "docType": "PLT",
                        "actionCode": row[1],
                        "locationCode": row[2],
                        "platformID": row[3],
                        "startDate": row[4],
                        "endDate": row[5],
                        "length": row[6],
                        "powerSupplyType": row[7],
                        "DDOPassenger": row[8],
                        "DDONonPassenger": row[9],
                    }            
                    totalImportedDocuments[4] += 1
                elif row[0] == "NWK":
                    # REFTYPE : Network Link
                    out_file = {
                        "docType": "NWK",
                        "actionCode": row[1],
                        "originLocation": row[2],
                        "destinationLocation": row[3],
                        "lineCode": row[4],
                        "lineDescription": row[5],
                        "startDate": row[6],
                        "endDate": row[7],
                        "initialDirection": row[8],
                        "finalDirection": row[9],
                        "DDOPassenger": row[10],
                        "DDONonPassenger": row[11],
                        "RETB": row[12],
                        "zone": row[13],
                        "reversible": row[14],
                        "powerSupplyType": row[15],
                        "RA": row[16],
                        "maxTrainLength": row[17],
                    }
                    totalImportedDocuments[5] += 1
                elif row[0] == "TLK":
                    # REFTYPE : Timing Link
                    out_file = {
                        "docType": "NWK",
                        "actionCode": row[1],
                        "originLocation": row[2],
                        "destinationLocation": row[3],
                        "lineCode": row[4],
                        "tractionType": row[5],
                        "trailingLoad": row[6],
                        "speed": row[7],
                        "RA": row[8],
                        "entrySpeed": row[9],
                        "exitSpeed": row[10],
                        "startDate": row[11],
                        "endDate": row[12],
                        "secRunTime": row[13],
                        "description": row[14],
                    }
                    totalImportedDocuments[6] += 1
                else:
                    out_file = {
                        "docType": "DEL"
                    }
                    totalImportedDocuments[7] += 1
                    
                # Append Copy of Dictionary to List
                bulkStore.append(out_file.copy())


                if ((sum(totalImportedDocuments) % self.inst['standard-bulk-size']) == 0):
                    
                    # InsertMany
                    db_confirm = self.mongodb['reference'].insert(bulkStore)
                    # Reset Bulk Storage
                    bulkStore = []
                    # Increment Counter
                    bulkCounter +=1

                    self.logg.info('REFERENCE Progress | {:.0%} '.format(sum(totalImportedDocuments)/1.2e6))  

        self.logg.info('Completed Reference Import Successfully')          


            
                  
       

    def importCORPUS(self):
        self.logg.info('Starting CORPUS Import')
        # Download CORPUS File
        if ~os.path.exists(r'data/corpus.json.gz'):
            r = requests.get('http://datafeeds.networkrail.co.uk/ntrod/SupportingFileAuthenticate?type=CORPUS',
                            auth=(self.auth[0], self.auth[1]))
            self.logg.info('CORPUS HTTP Request [%i] | Downloading File' % r.status_code)
            open(r'data/corpus.json.gz', 'wb').write(r.content)
        else:
            self.logg.info('CORPUS File Detected, skipping download')
            
        # Import CORPUS File to MongoDB
        
        totalImportedDocuments = 0
        
        
        self.logg.info('Reading CORPUS File')
        with gzip.open(r'data/corpus.json.gz', "rt", encoding="utf-8") as f:
            data = json.load(f)
            
        self.logg.info('Importing CORPUS into MongoDB')
        for entry in data['TIPLOCDATA']:
            db_confirm = self.mongodb['tiploc'].insert_one(entry) 
            totalImportedDocuments += 1
            if ((totalImportedDocuments%5000) == 0):
                self.logg.info('CORPUS Progress | {:.0%}'.format(totalImportedDocuments/6e4))  
        self.logg.info('Completed CORPUS Import Successfully')  
        
        

    def importFullSchedule(self):
        """NRDF Import Schedule 

            1. Check if file downlaoded
            2. Read file into local .json format
            3. Use local .json files to populate MongoDB using bulk inserts

        """
        self.logg.info('Starting SCHEDULE Import')

        # Check if file already downloaded
        if ~os.path.exists(r'data/toc-full.json.gz'):
            request_url = 'https://datafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_FULL_DAILY&day=toc-full'
            self.logg.info('Downloading File | {0}'.format(request_url))

            r = requests.get(request_url,auth=(self.auth[0], self.auth[1]))

            self.logg.info('SCHEDULE HTTP Request [%i] | File Downloaded' % r.status_code)
            open(r'data/toc-full.json.gz', 'wb').write(r.content)
        else:
            self.logg.info('SCHEDULE File Detected, skipping download')

        sch = Schedule(self.mongodb, self.logg, self.inst)

        self.logg.info('Reading Schedule File')
        sch.readTimetableVersion()
        sch.readAssociation()
        sch.readTiploc()
        sch.readSchedule()
        self.logg.info('Completed Writing Files to /data \n Starting Importing to MongoDB')
        sch.importSchedule()
        sch.importTiploc()
        sch.importAssociation()
        self.logg.info('Completed MongoDB Import')