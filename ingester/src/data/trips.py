from pyspark.sql import DataFrame

from conf.config import config
from core.spark import spark

class Trips:
    def __init__(self):
        self._table_name = config['TRIPS_TABLE']
        self._spark = spark
        self._connection_string = f"jdbc:postgresql://{config['DB_HOST']}:{config['DB_PORT']}/{config['DB_NAME']}"

    def read_dataframe(self) -> DataFrame:
        """Get trips table as a DataFram"""

        return self._spark.read \
            .format("jdbc") \
            .option("url", self._connection_string) \
            .option("dbtable", self._table_name) \
            .option("user", config['DB_USER']) \
            .option("password", config['DB_PASSWORD']) \
            .load()
    
    def append_dataframe(self, df: DataFrame) -> None:
        """Append DataFrame to trips table"""

        df.write \
            .format("jdbc") \
            .option("url", self._connection_string) \
            .option("dbtable", self._table_name) \
            .option("user", config['DB_USER']) \
            .option("password", config['DB_PASSWORD']) \
            .mode("append") \
            .save()