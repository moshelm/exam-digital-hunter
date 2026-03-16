from pydantic import BaseModel
from datetime import datetime

class Damage(BaseModel):
    timestamp: str
    attack_id: str
    entity_id: str
    result:str
