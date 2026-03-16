from shared.schemas.attack_schema import Attack
from shared.schemas.intel_schema import Intel
from shared.schemas.damage_schema import Damage
from shared.logger import log_event

class Validator():
    def __init__(self, categories:list[str]):
        self.categories = categories

    def validation_by_category(self,cat, data)->dict:
        try:
            result = {'is_valid':True,'info':''}
            if cat not in self.categories:
                result['is_valid'] = False
                result['info'] = 'source data not familiar'
            if not self.validate_fields(cat,data):
                result['is_valid'] = False
                result['info'] = 'error in fields of this data'
            return result
        except Exception:
            log_event('error','failed validation by category')

    def validate_fields(self, cat:str ,data:dict)->bool:
        try:
            if cat == 'intel':
                Intel.model_validate(**data)
            elif cat == 'attack':
                Attack.model_validate(**data)
            elif cat == 'damage':
                Damage.model_validate(**data)
            return True
        except Exception:
            return False