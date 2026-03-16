from confluent_kafka import Producer, Message
from shared.logger import log_event
from shared.utils.serializer import serialize_json

class KafkaProducer():
    def __init__(self,kafka_config:dict, topic_name:str):
        try:
            self.producer = Producer(kafka_config)
            log_event('info','success connect to kafka')
        except Exception:
            log_event('error','failed connect to kafka')
            raise
        self.topic = topic_name

    def run(self, data):
        try:
            value = serialize_json(data)
            self.producer.produce(self.topic,value, callback=self.delivery)
            log_event('info','new event send in success')
        except Exception:
            log_event('warning',"failed to sent new event")
    def delivery(self, err: Message, msg:Message):
        if err:
            log_event('warning',f'failed to sent new event.{err}')
        else:
            log_event('info','success sending new event')
    def flush(self):
        try:
            self.producer.flush()
            log_event('info','flushing all events')
        except Exception:
            log_event('error','failed to flush')


