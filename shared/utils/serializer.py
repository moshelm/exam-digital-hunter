import json 
from shared.logger import log_event

def serialize_json(data):
    try:
        serialize_data = json.dumps(data).encode()
        log_event('info','success serialize data')
        return serialize_data
    except Exception:
        log_event('warning','failed to serialize data')

def deserialize_json(data):
    try:
        if isinstance(data,bytes):
            data = data.decode()
        deserialize_data = json.loads(data)
        log_event('info','success deserialize')
        return deserialize_data
    except Exception:
        log_event('warning','failed deserialize')