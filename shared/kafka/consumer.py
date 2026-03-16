from confluent_kafka import Consumer, Message
from logging import Logger 
from utils.serializer import deserialize_json


class KafkaConsumer():
    def __init__(self,kafka_config:dict, topics:list[str], logger:Logger):
        self.logger = logger
        try:
            self.consumer = Consumer(kafka_config)
            self.logger.info('success connect to kafka')
        except Exception:
            self.logger.critical('failed connect to kafka')
            raise
        self.topics = topics

    def run(self,callback):
        try:
            self.consumer.subscribe(self.topics)
            while True:
                msg = self.consumer.poll()
                if not msg:
                    continue
                if msg.error():
                    self.logger.error(f'there is a error {msg.error()}')
                data = deserialize_json(msg.value())
                callback(data)
        except Exception:
            self.logger.critical('consumer failed running',exc_info=True)
            raise
