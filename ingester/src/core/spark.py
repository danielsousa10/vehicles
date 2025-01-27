from pyspark.sql import SparkSession

from conf.config import config

class Spark:
    def __init__(self, name=''):
        self.session = self.get_session(name)

    def get_session(self, name: str) -> SparkSession:
        """Get spark session"""

        session = SparkSession\
                .builder\
                .master("local[*]")\
                .appName(name)\
                .getOrCreate()

        return session
    
spark = Spark(config['PIPELINE_NAME']).session