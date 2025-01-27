import logging

from datetime import datetime
from pyspark.sql import DataFrame, functions as F 

from conf.config import config
from data.trips import Trips
from core.spark import spark

logger = logging.getLogger("ingester")

class Ingester:
    def __init__(self):
        self._path = config['FILES_PATH']
        self._spark = spark
        self._trips = Trips()
    
    def adjust_columns(self, df: DataFrame) -> DataFrame:
        adj_coord = lambda x: F.replace(
                                    F.replace(F.col(x), F.lit("POINT ("), F.lit("")),
                                F.lit(")"), F.lit(""))

        df = df.withColumn("origin_coord", adj_coord("origin_coord"))\
            .withColumn("destination_coord", adj_coord("destination_coord"))

        origin_col = F.split(F.col("origin_coord"), ' ')
        destination_col = F.split(F.col("destination_coord"), ' ')

        df = df.withColumn("origin_coord_x", origin_col.getItem(0).cast("DECIMAL(20, 17)"))\
            .withColumn("origin_coord_y", origin_col.getItem(1).cast("DECIMAL(20, 17)"))\
            .withColumn("destination_coord_x", destination_col.getItem(0).cast("DECIMAL(20, 17)"))\
            .withColumn("destination_coord_y", destination_col.getItem(1).cast("DECIMAL(20, 17)"))\
            .withColumn("datetime", F.to_timestamp("datetime"))\
            .drop("origin_coord", "destination_coord")

        return df

    def run(self) -> None:
        """Start ingester process"""

        try:
            start = datetime.now()
            logger.info(f"Started ingest process at {start}.")

            #Read all csv files from /data folder into a DataFrame 
            df = self._spark.read\
                    .options(delimiter=",", header=True)\
                    .csv(self._path)

            #Using the original coordinates columns, generate an (x, y) tuple for latitude and longitude.
            df = self.adjust_columns(df).cache()
            num_rows = df.count()

            self._trips.append_dataframe(df)

            df.unpersist()

            logger.info(f"Successfully ingested {num_rows} records at trips table in {datetime.now() - start}.")
        except Exception as e:
            logger.error(f"Failed to ingest data! Error: \n {e}")