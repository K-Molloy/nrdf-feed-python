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

    def __init__(self,db):
        # Save DB locally
        self.db = db

        self.type_counters = [0,0,0,0,0,0,0,0]

    def getCounters(self):
        # Return Counters
        return self.type_counters


    def on_message(self, message_txt):

        message_json = json.loads(message_txt)

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
        query_res = self.db['schedule'].find(query)

        # Setup DB Insertion
        activated_train = {
            "train_uid" : message['body']['train_uid'],
            "trust_id" : message['body']['train_id'],
            "schedule_id" : query_res[0]['_id'],
            "operator_id" : message['body']['toc_id'],
            "state" : "activated",
            "activation" : db_confirm.inserted_id,
            "movements" : []
        }

        db_confirm = self.db['trains'].insert_one(activated_train)





    def trainCancellation(self,message):
        # Handler Function for 
        # 0002 - Train Cancellation

        # Insert into trust_cancellations DB
        db_confirm = self.db['trust_cancellations'].insert_one(message['body'])

        # Find train with trust_id equivalent
        query = {
            "trust_id": message['body']['train_id'],
            "operator" : message['body']['toc_id']
        }

        newState = {
            "$set": { "state": "cancelled" }, "$push" : { "movements" : db_confirm.inserted_id} 
        }

        # Find schedule that conform with activation and update
        db_confirm = self.db['trains'].update_one(query, newState, upsert=True)


    def trainMovement(self,message):
        # Handler Function for 
        # 0003 - Train Movement

        # Insert into trust_movements DB
        db_confirm = self.db['trust_movement'].insert_one(message['body'])

        # Find train with trust_id equivalent
        query = {
            "trust_id": message['body']['train_id'],
            "operator" : message['body']['toc_id']
        }

        newState = {
            "$set": { "state": "moving" }, "$push" : { "movements" : db_confirm.inserted_id} 
        }

        # Find schedule that conform with activation and update
        db_confirm = self.db['trains'].update_one(query, newState, upsert=True)

    def trainReinstatement(self,message):
        # Handler Function for 
        # 0005 - Train Reinstatement

        # Insert into trust_other DB
        db_confirm = self.db['trust_other'].insert_one(message['body'])

        # Find train with trust_id equivalent
        query = {
            "trust_id": message['body']['train_id'],
            "operator" : message['body']['toc_id']
        }

        newState = {
            "$set": { "state": "reinstated" }, "$push" : { "changes" : db_confirm.inserted_id} 
        }

        # Find schedule that conform with activation and update
        db_confirm = self.db['trains'].update_one(query, newState, upsert=True)
        
    def trainCOO(self,message):
        # Handler Function for 
        # 0006 - Change of Origin

        # Insert into trust_other DB
        db_confirm = self.db['trust_other'].insert_one(message['body'])

        # Find train with trust_id equivalent
        query = {
            "trust_id": message['body']['train_id'],
            "operator" : message['body']['toc_id']
        }

        newState = {
            "$set": { "state": "coo" }, "$push" : { "changes" : db_confirm.inserted_id} 
        }

        # Find schedule that conform with activation and update
        db_confirm = self.db['trains'].update_one(query, newState, upsert=True)

    def trainCOI(self,message):
        # Handler Function for 
        # 0007 - Change of Identity

        # Insert into trust movements DB
        db_confirm = self.db['trust_coi'].insert_one(message['body'])

        # Find train with trust_id equivalent
        query = {
            "trust_id": message['body']['train_id'],
            "operator" : message['body']['toc_id']
        }

        newState = {
            "$set": { "state": "coi" }, "$push" : { "changes" : db_confirm.inserted_id} 
        }

        # Find schedule that conform with activation and update
        db_confirm = self.db['trains'].update_one(query, newState, upsert=True)

    def trainCOL(self,message):
        # Handler Function for 
        # 0008 - Change of Location

        # Insert into trust_other DB
        db_confirm = self.db['trust_other'].insert_one(message['body'])

        # Find train with trust_id equivalent
        query = {
            "trust_id": message['body']['train_id'],
            "operator" : message['body']['toc_id']
        }

        newState = {
            "$set": { "state": "col" }, "$push" : { "changes" : db_confirm.inserted_id} 
        }

        # Find schedule that conform with activation and update
        db_confirm = self.db['trains'].update_one(query, newState, upsert=True)
