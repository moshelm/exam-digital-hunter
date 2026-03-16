from pymongo import MongoClient
from logging import Logger
from shared.logger import log_event

class MongodbManager():
    def __init__(self,config:str,database: str):
        try:
            client = MongoClient(config)
            log_event('info','success connect to mongodb server')
            self.db = client[database]
        except Exception:
            log_event('error','failed connect to mongodb server')
            raise
