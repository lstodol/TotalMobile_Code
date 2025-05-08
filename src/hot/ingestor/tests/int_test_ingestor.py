import unittest
from unittest.mock import patch
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType,
    MapType,
)
from pyspark.dbutils import DBUtils
import multiprocessing
import time
import random
from threading import Timer
import fixpath
from hot.ingestor.usecase.interactor import new_interactor
from hot.ingestor.storage.repository import new_repository, Repository
from hot.ingestor.handler.processor import new_processor
from modules.authorisation import Authorisation

class TestIngestor(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Setup method called once before all test methods"""
        cls.spark = SparkSession.getActiveSession()
        cls.dbutils =  DBUtils(cls.spark)
        
        cls.tenant_name = "inttestingestor1"
        cls.product_name = "totalmobile"
        cls.event_hub_namespace = "dev-analytics-u-uks-evhns"
        cls.entity_name = "bulletin"

        cls.cleanup()
        cls.create_test_db()

        rate_df = cls.spark.readStream.format("rate").option("rowsPerSecond", 100).load()
        rate_df = rate_df.withColumn("body", F.lit("""{
                    "unique_key":"the_key_1",
                    "category":"Bulletin",
                    "subject":"action to be taken 1",
                    "event_time":"2024-08-12T13:15:38.267+00:00",
                    "status":1,
                    "recipients":[
                            {
                                "user_key":"bb4a5b02-e0f2-479e-9926-11f342ff6387",
                                "attributes":[
                                        {"key":"key","value":"value"}
                                    ],
                                "name":"user1"
                            }
                        ]
                    }"""))
        rate_df = rate_df.withColumn("timestamp", F.lit(datetime.strptime("2024-08-12 10:28:05", "%Y-%m-%d %H:%M:%S")))
        rate_df = rate_df.withColumn("partition", F.lit(0))
        rate_df = rate_df.withColumn("offset", F.lit(11640))    
        rate_df = rate_df.withColumn("sequenceNumber", F.lit(24))
        rate_df = rate_df.withColumn("publisher", F.lit(None))
        rate_df = rate_df.withColumn("partitionKey", F.lit(None))
        tenant_id_udf = F.udf(lambda x: f"inttestingestor{random.choice((0, 1))}" , StringType())
        rate_df = rate_df.withColumn("tenant_id", tenant_id_udf(F.lit("inttestingestor")))
        rate_df = rate_df.withColumn("properties", F.struct(F.col("tenant_id"))) 
        rate_df = rate_df.withColumn("x-opt-sequence-number-epoch",F.lit("-1"))  
        rate_df = rate_df.withColumn("systemProperties",  F.struct(F.col("x-opt-sequence-number-epoch")))
        rate_df = rate_df.withColumn("load_date", F.current_timestamp())

        cls.stream_df = rate_df

    @classmethod 
    def create_test_db(cls):
        
        cls.spark.sql(f"CREATE DATABASE {cls.tenant_name}")
        
        cls.spark.sql(f"""CREATE TABLE {cls.tenant_name}.bronze_{cls.product_name}_{cls.entity_name} (
                            unique_key STRING,
                            category STRING,
                            subject STRING,
                            event_time TIMESTAMP, 
                            status INT,
                            recipients ARRAY<STRUCT<user_key: STRING, name: STRING, attributes: ARRAY<MAP<STRING, STRING>>>>,
                            load_date TIMESTAMP
                        )
                        USING DELTA
                        TBLPROPERTIES (
                            'delta.enableChangeDataFeed' = 'true',
                            'delta.autoOptimize.autoCompact'='true', 
                            'delta.autoOptimize.optimizeWrite'='true'
                        )""")

    @classmethod
    def drop_test_db(cls):
        cls.spark.sql(f"DROP DATABASE IF EXISTS {cls.tenant_name} CASCADE")
    
    @patch.object(Repository, "read_stream")
    def test_write_stream_for_bulletin_entity(self, mock_read_stream):
        connection_string = self.dbutils.secrets.get(Authorisation.SECRET_SCOPE, f"sas-{self.event_hub_namespace}-{self.entity_name}-listen-evh-connection")

        mock_read_stream.return_value = self.stream_df
        repository = new_repository(connection_string=connection_string, spark_session=self.spark)
        interactor = new_interactor(repository=repository, spark_session=self.spark)
        processor = new_processor(interactor=interactor)

        processor.handler(self.tenant_name, self.product_name, self.entity_name)

        # run streaming query for 20 seconds
        time.sleep(20)
        interactor.entity_streaming_query.stop()

        sink_df = self.spark.sql(f"select * from {self.tenant_name}.bronze_{self.product_name}_{self.entity_name}")
        records_count = sink_df.count()

        #running query for 20 seconds, with 100 rows per second and probability of 50% rows being generated for tested tenant 
        #should result in around 1000 records in ideal world, but randomnes is unknonw so we are testing for 600-1400 range
        #which is very less likly
        self.assertGreater(records_count, 600, "Records count is less than 600")
        self.assertGreater(1400, records_count, "Records count is greater than 1400")

    @classmethod
    def tearDownClass(cls):
        """Teardown method called once after all test methods"""
        cls.cleanup()

    @classmethod
    def cleanup(cls):
        cls.drop_test_db()
        cls.dbutils.fs.rm(f"dbfs:/mnt/{cls.tenant_name}", True)
        cls.dbutils.fs.rm(f"dbfs:/user/hive/warehouse/{cls.tenant_name}.db", True)


if __name__ == "__main__":
    test_runner = unittest.main(argv=[""], exit=False)
    test_runner.result.printErrors()