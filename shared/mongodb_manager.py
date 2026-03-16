from pymongo import MongoClient
from logging import Logger


class MongodbManager():
    def __init__(self,config:str,database: str, logger: Logger ):
        self.logger = logger 
        try:
            client = MongoClient(config)
            self.db = client[database]
        except Exception:
            self.logger.critical('failed connect to mongodb server',exc_info=True)
            raise
