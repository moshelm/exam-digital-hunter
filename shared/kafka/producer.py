from confluent_kafka import Producer, Message
from logging import Logger 
from utils.serializer import serialize_json

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

    def run(self, data):
        try:
            value = serialize_json(data,self.logger)
            self.producer.produce(self.topic,value, callback=self.delivery)
            self.logger.info('new event send in success')
        except Exception:
            self.logger.error("failed to sent new event",exc_info=True)
    def delivery(self, err: Message, msg:Message):
        if err:
            self.logger.error(f'failed to sent new event.{err}')
        else:
            self.logger.info('success sending new event')
    def flush(self):
        try:
            self.producer.flush()
            self.logger.info('flushing all events')
        except Exception:
            self.logger.error('failed to flush')


