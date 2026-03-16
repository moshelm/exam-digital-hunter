import os 



class Configuration():
    def __init__(self):
        mongodb_host = os.getenv("MONGODB_HOST",'localhost') 
        mongodb_port = os.getenv("MONGODB_PORT",'27017') 
        self.mongodb_database = os.getenv("MONGODB_DATABASE",'digital_hunter') 
        self.mongodb_collection_base = os.getenv("MONGODB_COLLECTION",'targets bank') 
        
        kafka_host = os.getenv("KAFKA_HOST",'localhost') 
        kafka_port = os.getenv("KAFKA_PORT",'9092') 
        kafka_group_id = os.getenv('KAFKA_GROUP_ID','extract')
        
        self.kafka_producer_topic = os.getenv('KAFKA_PRODUCER_TOPIC','')
        self.kafka_consumer_topics = os.getenv('KAFKA_CONSUMER_TOPICS','intel,attack,damage').split(',')

        self.kafka_bootstrap = {'bootstrap.servers':f'{kafka_host}:{kafka_port}'}
        self.kafka_group = kafka_group_id

        self.mongodb_config = f'mongodb://{mongodb_host}:{mongodb_port}'