import logging

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, Connection

from conf.config import config

class Database:
    def __init__(self):
        self._connection_string = (f"postgresql+psycopg2://{config['DB_USER']}:" +
            f"{config['DB_PASSWORD']}@" +
            f"{config['DB_HOST']}:" +
            f"{config['DB_PORT']}/" +
            f"{config['DB_NAME']}")

        self._engine = self.get_endine()
        self.connection = self.connect()

    def get_endine(self) -> Engine:
        return create_engine(self._connection_string)

    def connect(self) -> Connection:
        """Connect to database"""

        result = None
        logging.info("Connecting to database.")
        
        try:
            result = self._engine.connect()
        except Exception as e:
            logging.error(f"Failed to connect to database with error:\n {e}.")
        
        return result
    
    def close(self):
        """Close connection with database"""
        self.connection.close()