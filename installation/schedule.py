import json
import gzip
import os 


class Schedule:

    def __init__(self,db,logrep,_config):
        self.mongodb = db
        self.logg = logrep
        self.conf = _config

    

    def readAssociation(self):
        # File is invalid JSON so extra parsing is required
        # Counter
        totalImportedDocuments = 0
        # Bulk Storage ; List of Dicts
        bulkStore = []
        # Open .gz file

        self.logg.info('Starting Association Reading')
        with gzip.open('data/toc-full.gz', "rt", encoding="cp1252") as f:

            # Iterate over lines
            for obj in f:

                currentObject = json.loads(obj)

                # Find "JsonAssociationV1" entries
                if "JsonAssociationV1" in currentObject:

                    totalImportedDocuments += 1

                    bulkStore.append(currentObject.copy())

                if (totalImportedDocuments % 5000 == 0 and totalImportedDocuments != 0):
                    self.logg.info('Association Progress | {:.0%}'.format(totalImportedDocuments/50000))   


            self.logg.info('Starting File Writing | Total Documents {}'.format(totalImportedDocuments))
            with open('data/sch/association.json', 'w') as fn:
                json.dump(bulkStore, fn)
        # Association all written to file
        return True

    def readTiploc(self):
        # File is invalid JSON so extra parsing is required

        # Counter
        totalImportedDocuments = 0

        # Bulk Storage ; List of Dicts
        bulkStore = []

        self.logg.info('Starting Tiploc Reading')
        # Open .gz file
        with gzip.open('data/toc-full.gz', "rt", encoding="cp1252") as f:

            # Iterate over lines
            for obj in f:

                currentObject = json.loads(obj)

                # Find "TiplocV1" entries
                if "TiplocV1" in currentObject:

                    totalImportedDocuments += 1

                    bulkStore.append(currentObject.copy())

                if (totalImportedDocuments %5000 == 0 and totalImportedDocuments != 0):
                    self.logg.info('Tiploc Progress | {:.0%}'.format(totalImportedDocuments/10000)) 

            self.logg.info('Starting File Writing | Total Documents {}'.format(totalImportedDocuments))
            with open('data/sch/tiploc.json', 'w') as fn:
                json.dump(bulkStore, fn)
        # Tiploc all written to file
        return True


    def readSchedule(self):
        # File is invalid JSON so extra parsing is required

        # Total Counter
        totalImportedDocuments = 0
        # Individual File Counter
        currentImportedDocuments = 0

        # Bulk Storage ; List of Dicts
        bulkStore = []
        bulkCounter = 0

        self.logg.info('Starting Schedule Reading')

        self.logg.info('Bulk Size : {0}'.format(self.conf['bulk-size']))
        # Open .gz file
        with gzip.open('data/toc-full.gz', "rt", encoding="cp1252") as f:

            # Iterate over lines
            for obj in f:

                currentObject = json.loads(obj)

                # Find "JsonScheduleV1" entries
                if "JsonScheduleV1" in currentObject:

                    # Increment Counter
                    totalImportedDocuments += 1
                    currentImportedDocuments += 1

                    # Append copy of dict to list
                    bulkStore.append(currentObject.copy())

                if (totalImportedDocuments %5000 == 0 and totalImportedDocuments != 0):
                    self.logg.info('Schedule Progress | {:.0%}'.format(currentImportedDocuments/self.conf['bulk-size']))

                # Requires extra condition
                if (totalImportedDocuments % self.conf['bulk-size'] == 0 and totalImportedDocuments != 0):

                    self.logg.info('Bulk Limit | Writing to File | Total Bulks {}'.format(bulkCounter))

                    self.writeScheduleToFile(bulkCounter, bulkStore)
                    
                    # Reset List
                    bulkStore = []
                    # Increment Number of Bulks / Number of Files + 1
                    bulkCounter += 1
                    # Reset File Counter
                    currentImportedDocuments = 0

        # Write any remaining to file
        self.writeScheduleToFile(bulkCounter, bulkStore)

        self.logg.info('Total schedule#.json Files {}'.format(bulkCounter+1))
        # Return True as Completed Flag
        return True

    def writeScheduleToFile(self,bulkCounter, bulkStore):
        """Writes Schedule Entries to file

        Args:
            bulkCounter ([integer]): [Iterative File Number]
            bulkStore ([list]): [Files to be written]
        """

        with open('data/sch/schedule{}.json'.format(bulkCounter), 'w') as fn:
            json.dump(bulkStore, fn)


    def readTimetableVersion(self):
        """Reads first line of schedule file and imports into MongoDB

        Returns:
            [Boolean]: [Completion Flag]
        """
        # JsonTimetableV1 is always in the first line 
        with gzip.open('data/toc-full.gz', "rt", encoding="cp1252") as f:

            line = f.readline()

            currentObject = json.loads(line)

            with open('data/sch/version.json', 'w') as fn:
                json.dump(currentObject, fn)

        return True


    def importAssociation(self):
        """Takes association.json and imports file into MongoDB
        """

        # Initialise Empty Lists 
        unchangedFile = []
        bulkStore = []

        # Open File
        with open('data/sch/association.json') as f:
            # Read File in j
            for line in f:
                unchangedFile.append(json.loads(line))

            for entry in unchangedFile[0]:
                if entry['JsonAssociationV1']['transaction_type'] == 'Create':
                    del entry['JsonAssociationV1']['transaction_type']
                    bulkStore.append(entry['JsonAssociationV1'])
                elif entry['JsonAssociationV1']['transaction_type'] == 'Delete':
                    # Need to configure Deleting stuff
                    False


            db_confirm = self.mongodb['association'].insert(bulkStore)

        return db_confirm


    def importTiploc(self):
        """Takes tiploc.json and imports into MongoDB
        
        Currently Unused, unsure if working
        """

        # Initialise Empty Lists 
        unchangedFile = []
        bulkStore = []

        # Open File
        with open('data/sch/tiploc.json') as f:
            # Read File in j
            for line in f:
                unchangedFile.append(json.loads(line))

            for entry in unchangedFile[0]:
                if entry['TiplocV1']['transaction_type'] == 'Create':
                    del entry['TiplocV1']['transaction_type']
                    bulkStore.append(entry['TiplocV1'])
                elif entry['TiplocV1']['transaction_type'] == 'Delete':
                    # Need to configure Deleting stuff
                    False


            db_confirm = self.mongodb['tiploc2'].insert(bulkStore)

        return db_confirm
