# Handler Functions for messages from the TRUST Feed

# Message Types
# 0001 - Train Activation
# 0002 - Train Cancellation
# 0003 - Train Movement
# 0004 - Unused
# 0005 - Train Reinstatement
# 0006 - Change of Origin
# 0007 - Change of Identity
# 0008 - Change of Location


# TRUST Collections
#  
# trust_activations
#   id
#   trust_id
#   train_uid
#   schedule_id 
#   movements []
#   
# trust_movements
#   as network rail send
#
# trust_cancellations
#   as network rail send
#
# trust_other
#   type
#   as network rail send

import json

class TRUST:

    def __init__(self,db,logg):
        # Save DB locally
        self.db = db

        # Save Logging Object
        self.logg = logg

        # Initialise Message Counters
        self.type_counters = [0,0,0,0,0,0,0,0]

    def getCounters(self):
        # Return Counters
        return self.type_counters


    def on_message(self, message_txt):

        try:
            message_json = json.loads(message_txt)
        except:
            self.logg.error('Invalid JSON File')

        # Each 'message' can have multiple trains
        for item in message_json:

            # Determine Message Type
            if item['header']['msg_type'] == '0001':
                self.type_counters[0] += 1
                self.trainActivation(item)

            if item['header']['msg_type'] == '0002':
                self.type_counters[1] += 1
                self.trainCancellation(item)

            if item['header']['msg_type'] == '0003':
                self.type_counters[2] += 1
                self.trainMovement(item)

            if item['header']['msg_type'] == '0005':
                self.type_counters[3] += 1
                self.trainReinstatement(item)

            if item['header']['msg_type'] == '0006':
                self.type_counters[4] += 1
                self.trainCOO(item)

            if item['header']['msg_type'] == '0007':
                self.type_counters[5] += 1
                self.trainCOI(item)

            if item['header']['msg_type'] == '0008':
                self.type_counters[6] += 1
                self.trainCOL(item)




        

        None

    def trainActivation(self,message):
        # Handler Function 
        # 0001 - Train Activation

        # Insert into trust_activations DB
        db_confirm = self.db['trust_activations'].insert_one(message['body']) 

        # Setup DB query
        query = {
        "CIF_train_uid": message['body']['train_uid'],
        "schedule_start_date": message['body']['schedule_start_date']
        }

        # Find all schedules that conform with activation
        query_res = self.db['schedule'].find_one(query)

        try:
            associated_schedule = query_res['_id']
            self.logg.debug('ACT PASS TRUST {} | SCHED {}'.format(message['body']['train_id'], associated_schedule))
        except TypeError as e:
            associated_schedule = 'UNKNOWN'
            self.logg.error('ACT FAIL TRUST {} | SCHED UNKNWN '.format(message['body']['train_id']))

        else:

            # Setup DB Insertion
            activated_train = {
                "train_uid" : message['body']['train_uid'],
                "trust_id" : message['body']['train_id'],
                "schedule_id" : associated_schedule,
                "train_service_code" : message['body']['train_service_code'],
                "state" : "activated",
                "activation" : db_confirm.inserted_id,
            }

            db_confirm = self.db['trains'].insert_one(activated_train)



    def trainCancellation(self,message):
        # Handler Function for 
        # 0002 - Train Cancellation

        # Insert into trust_cancellations DB
        db_confirm = self.db['trust_cancellations'].insert_one(message['body'])

        # Find train with trust_id equivalent
        query = {
            "trust_id": message['body']['train_id']
        }

        # Locate Train Record
        query_res = self.db['trains'].find_one(query)

        # Find Train in db['train'] and update information, if not there create it
        try:
            # Get query ObjectID of Train records
            associatedTrain = query_res['_id']
            self.logg.debug('CAN PASS TRUST {} | TRAIN {} '.format(message['body']['train_id'], associatedTrain))

        except TypeError:
            self.logg.error('CAN FAIL TRUST {} | Creating Entry... '.format(message['body']['train_id']))
            # Create Train with suboptimal information
            associatedTrain = self.createUnactivatedTrain(activation='can', movement=db_confirm.inserted_id, message=message)

        else:

            # Save ObjectID and create new query
            updateQuery = {
                "_id" : associatedTrain
            }

            # New State 
            newState = {
                "$set": { "state": "cancelled" },
                "$push" : { "movements" : db_confirm.inserted_id} 
            }

            # Find schedule that conform with activation and update
            db_confirm = self.db['trains'].update_one(updateQuery, newState)


    def trainMovement(self,message):
        # Handler Function for 
        # 0003 - Train Movement

        # Insert into trust_movements DB
        db_confirm = self.db['trust_movement'].insert_one(message['body'])

        # Find train with trust_id equivalent
        query = {
            "trust_id": message['body']['train_id']
        }

        # Locate Train Record
        query_res = self.db['trains'].find_one(query)

        
        # Find Train in db['train'] and update information, if not there create it
        try:
            # Get query ObjectID of Train records
            associatedTrain = query_res['_id']
            self.logg.debug('MOV PASS TRUST {} | TRAIN {}'.format(message['body']['train_id'], associatedTrain))

        except TypeError:
            self.logg.error('MOV FAIL TRUST {} | Creating Entry... '.format(message['body']['train_id']))
            # Create Train with suboptimal information
            associatedTrain = self.createUnactivatedTrain(activation='mov', movement=db_confirm.inserted_id, message=message)

        else:
            
            # Save ObjectID and create new query
            updateQuery = {
                "_id" : associatedTrain
            }

            if message['body']['train_terminated']=="false":
                state = 'moving'
            else:
                state = 'terminated'

            # New State is constant
            newState = {
                "$set": { "state": state },
                "$push" : { "movements" : db_confirm.inserted_id} 
            }

            # Find schedule that conform with activation and update
            db_confirm = self.db['trains'].update_one(updateQuery, newState)


    def trainReinstatement(self,message):
        # Handler Function for 
        # 0005 - Train Reinstatement

        # Insert into trust_other DB
        db_confirm = self.db['trust_other'].insert_one(message['body'])

        # Find train with trust_id equivalent
        query = {
            "trust_id": message['body']['train_id']
        }

        # Locate Train Record
        query_res = self.db['trains'].find_one(query)

        try:
            # Get query ObjectID of Train records
            associatedTrain = query_res['_id']
            self.logg.debug('REI PASS TRUST {} | TRAIN {} '.format(message['body']['train_id'], associatedTrain))

        except TypeError:
            self.logg.error('REI FAIL TRUST {} | Creating Entry... '.format(message['body']['train_id']))
            # Create Train with suboptimal information
            associatedTrain = self.createUnactivatedTrain(activation='rei', movement=db_confirm.inserted_id, message=message)

        else:

            # Save ObjectID and create new query
            updateQuery = {
                "_id" : associatedTrain
            }

            # New State 
            newState = {
                "$set": { "state": "reinstated" },
                "$push" : { "movements" : db_confirm.inserted_id} 
            }

            # Find schedule that conform with activation and update
            db_confirm = self.db['trains'].update_one(updateQuery, newState)
        
    def trainCOO(self,message):
        # Handler Function for 
        # 0006 - Change of Origin

        # Insert into trust_other DB
        db_confirm = self.db['trust_other'].insert_one(message['body'])

        # Find train with trust_id equivalent
        query = {
            "trust_id": message['body']['train_id']
        }

        # Locate Train Record
        query_res = self.db['trains'].find_one(query)

        # Find Train in db['train'] and update information, if not there create it
        try:
            # Get query ObjectID of Train records
            associatedTrain = query_res['_id']
            self.logg.debug('COO PASS TRUST {} | TRAIN {}'.format(message['body']['train_id'], associatedTrain))

        except TypeError:
            self.logg.error('COO FAIL TRUST {} | Creating Entry... '.format(message['body']['train_id']))
            # Create Train with suboptimal information
            associatedTrain = self.createUnactivatedTrain(activation='coo', movement=db_confirm.inserted_id, message=message)

        else:
            
            # Save ObjectID and create new query
            updateQuery = {
                "_id" : associatedTrain
            }

            # New State is constant
            newState = {
                "$set": { "state": "coo" },
                "$push" : { "movements" : db_confirm.inserted_id} 
            }

            # Find schedule that conform with activation and update
            db_confirm = self.db['trains'].update_one(updateQuery, newState)

    def trainCOI(self,message):
        # Handler Function for 
        # 0007 - Change of Identity

        # Insert into trust movements DB
        db_confirm = self.db['trust_other'].insert_one(message['body'])

        # Find train with trust_id equivalent
        query = {
            "trust_id": message['body']['train_id']
        }

        # Locate Train Record
        query_res = self.db['trains'].find_one(query)

        try:
            # Get query ObjectID of Train records
            associatedTrain = query_res['_id']
            self.logg.debug('COI PASS TRUST {} | TRAIN {} '.format(message['body']['train_id'], associatedTrain))

        except TypeError:
            self.logg.error('COI FAIL TRUST {} | Creating Entry... '.format(message['body']['train_id']))
            # Create Train with suboptimal information
            associatedTrain = self.createUnactivatedTrain(activation='coi', movement=db_confirm.inserted_id, message=message)

        else:

            # Save ObjectID and create new query
            updateQuery = {
                "_id" : associatedTrain
            }

            # New State 
            newState = {
                "$set": { "state": "coi" },
                "$push" : { "movements" : db_confirm.inserted_id} 
            }

            # Find schedule that conform with activation and update
            db_confirm = self.db['trains'].update_one(updateQuery, newState)

    def trainCOL(self,message):
        # Handler Function for 
        # 0008 - Change of Location

        # Insert into trust_other DB
        db_confirm = self.db['trust_other'].insert_one(message['body'])

        # Find train with trust_id equivalent
        query = {
            "trust_id": message['body']['train_id']
        }

        # Locate Train Record
        query_res = self.db['trains'].find_one(query)

        try:
            # Get query ObjectID of Train records
            associatedTrain = query_res['_id']
            self.logg.debug('COL PASS TRUST {} | TRAIN {} '.format(message['body']['train_id'], associatedTrain))

        except TypeError:
            self.logg.error('COL FAIL TRUST {} | Creating Entry... '.format(message['body']['train_id']))
            # Create Train with suboptimal information
            associatedTrain = self.createUnactivatedTrain(activation='col', movement=db_confirm.inserted_id, message=message)

        else:

            # Save ObjectID and create new query
            updateQuery = {
                "_id" : associatedTrain
            }

            # New State 
            newState = {
                "$set": { "state": "col" },
                "$push" : { "movements" : db_confirm.inserted_id} 
            }

            # Find schedule that conform with activation and update
            db_confirm = self.db['trains'].update_one(updateQuery, newState)


    def createUnactivatedTrain(self, activation, movement, message):

        # Setup DB Insertion
            activated_train = {
                "train_uid" : "UNKNOWN",
                "trust_id" : message['body']['train_id'],
                "train_service_code" : message['body']['train_service_code'],
                "state" : "UNKNOWN",
                "activation" : activation,
                "movements" : [movement]
            }

            db_confirm = self.db['trains'].insert_one(activated_train)

            return db_confirm.inserted_id