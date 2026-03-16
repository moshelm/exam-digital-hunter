from confluent_kafka import Producer
from logging import Logger 


class KafkaProducer():
    def __init__(self,kafka_config:dict, topic_name:str, logger:Logger):
        self.logger = logger
        try:
            self.producer = Producer(kafka_config)
            self.logger.info('success connect to kafka')
        except Exception:
            self.logger.critical('failed connect to kafka')
            raise
        self.topic = topic_name


