import json 
from logging import Logger

def serialize_json(data, logger: Logger):
    try:
        serialize_data = json.dumps(data).encode()
        logger.info('success serialize data')
        return serialize_data
    except Exception:
        logger.error('failed to serialize data',exc_info=True)
        
def deserialize_json(data,logger:Logger):
    try:
        if isinstance(data,bytes):
            data = data.decode()
        deserialize_data = json.loads(data)
        logger.info('success deserialize')
        return deserialize_data
    except Exception:
        logger.error('failed deserialize')