from shared.logger import log_event
from shared.kafka.consumer import KafkaConsumer
from shared.kafka.producer import KafkaProducer
from shared.mongodb_manager import MongodbManager
from server_config import Configuration
from orchestrator import Orchestrator
from validate_manager import Validator

config = Configuration()

def main():
    try:
        mongodb = MongodbManager(config.mongodb_config,config.mongodb_database)
        producer = KafkaProducer(config.kafka_bootstrap,config.kafka_producer_topic)
        consumer = KafkaConsumer(config.kafka_bootstrap, config.kafka_group, config.kafka_consumer_topics)
        validator = Validator(config.kafka_consumer_topics)
        log_event('info','create clients of servers')
        manager = Orchestrator(mongodb, producer, consumer, config.mongodb_collection_base, validator)

        manager.run()
    except Exception:
        log_event('error','main running failed')
    finally:
        if producer:
            producer.flush()
            log_event('info','flushing all events')
if __name__=='__main__':
    main()


