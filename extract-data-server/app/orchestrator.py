from shared.kafka.consumer import KafkaConsumer
from shared.kafka.producer import KafkaProducer
from shared.mongodb_manager import MongodbManager
from shared.logger import log_event
from validate_manager import Validator
from shared.haversine import haversine_km


class Orchestrator():
    def __init__(self, mongodb: MongodbManager, producer:KafkaProducer, consumer:KafkaConsumer, mongodb_collection_base:str, validator:Validator):
        self.consumer = consumer
        self.producer = producer
        self.mongodb = mongodb
        self.validator = validator
        self.collection_base = mongodb_collection_base

    def is_already_destroyed(self,event:dict):
        try:
            doc = {'entity_id':event['entity_id'],
                'status':'destroyed'}
            if self.mongodb.db[self.collection_base].find_one(doc):
                return True
            return False
        except Exception:
            log_event()

    def update_bank(self,topic:str, event:dict):
        try:
            entity_id = event['entity_id']
            doc_in_bank :dict = self.mongodb.db[self.collection_base].find_one(entity_id)
            new_details = {} 
            
            if topic =='intel':
                new_details['reported_lat'] = event['reported_lat'] 
                new_details['reported_lon'] = event['reported_lon'] 
                new_details['last_time_update_location'] = event['timestamp']
                new_details['priority_level'] = event['priority_level']
            if topic == 'attack':
                new_details['attacks_numbers'] = 1
            
            if topic == 'damage':
                new_details['status'] = event['result']
            
            if not doc_in_bank:
                new_details['priority_level'] = 99
                new_details['distance_from_last_location'] = 0
                self.mongodb.db[self.collection_base].insert_one(new_details)
            else:
                if doc_in_bank.get('attacks_numbers'):
                    new_details['attacks_numbers'] +=doc_in_bank['attacks_numbers']
                if doc_in_bank.get('last_time_update_location'):
                    if doc_in_bank['last_time_update_location'] > new_details['last_time_update_location']:
                        new_details['last_time_update_location'] = doc_in_bank['last_time_update_location']
                    else:
                        new_details['distance_from_last_location'] = haversine_km(doc_in_bank['reported_lat'],doc_in_bank['reported_lon'],event['reported_lat'],event['reported_lon'])

                if doc_in_bank.get('status'):
                    ["destroyed", "damaged", "no_damage"]
                    if doc_in_bank['status'] == "damaged" and new_details['status'] == 'no_damage':
                        new_details['status'] = doc_in_bank['status']
            
                self.mongodb.db[self.collection_base].update_one({'entity_id':entity_id},new_details,True)
        except Exception:
            log_event()


    def handle_event(self,topic:str, event:dict):
        try:
            result = self.validator.validation_by_category(topic, event)
            log_event('info','finish validate process')

            if not result['is_valid']:
                event['not_collect_information'] = result['info']
                self.producer.run(event)
                log_event('info','send event to topic intel_signals_dlq')

            elif self.is_already_destroyed(event):
                event['not_collect_information'] = 'the target already destroyed'
                self.producer.run(event)
                log_event('info','send event to topic intel_signals_dlq')

            else:
                self.mongodb.db[topic].insert_one(event) 
                self.update_bank(topic,event)
        except Exception:
            log_event()
                 
    def run(self):
        try:
            self.consumer.run(self.handle_event)
        except Exception:
            log_event('error','orchestrator stop running')
            raise