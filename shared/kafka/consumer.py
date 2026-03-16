from confluent_kafka import Consumer
from shared.utils.serializer import deserialize_json
from shared.logger import log_event

class KafkaConsumer():
    def __init__(self, kafka_config:dict, group_id:str, topics:list[str]):
        config = kafka_config
        config['group.id'] = group_id
        try:
            self.consumer = Consumer(config)
            log_event('info','success connect to kafka')
        except Exception:
            log_event('error','failed connect to kafka')
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
                    log_event('warning',f'error in event. {msg.error()}')
                    continue

                data = deserialize_json(msg.value())
                
                callback(msg.topic(), data)
        except Exception:
            log_event('error','consumer failed running')
            raise
