from pydantic import BaseModel
from datetime import datetime

class Damage(BaseModel):
    timestamp: datetime
    attack_id: str
    entity_id: str
    result:str
