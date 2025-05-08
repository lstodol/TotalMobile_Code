import unittest
import time
from unittest.mock import patch
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from string import Template
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType,
    MapType,
)
from pyspark.dbutils import DBUtils
import fixpath
from hot.domain.care.usecase.interactor import new_interactor
from hot.domain.care.storage.repository import new_repository, Repository
from hot.domain.care.handler.processor import new_processor
from modules.authorisation import Authorisation


def mock_build_table_paths(tenant_name: str, data_layer: str, table_name: str):
    table_path = Template("/user/hive/warehouse/${tenant_name}.db/${table_name}")
    checkpoint_path = Template("/mnt/${tenant_name}/${data_layer}_checkpoint/${tenant_name}.${table_name}")

    return table_path.substitute(tenant_name=tenant_name, table_name=table_name), checkpoint_path.substitute(tenant_name=tenant_name, table_name=table_name, data_layer=data_layer)


class TestCare(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Setup method called once before all test methods"""
        cls.spark = SparkSession.getActiveSession()
        cls.dbutils =  DBUtils(cls.spark)
        
        cls.tenant_name = "inttestcare1"
        cls.domain_name = "care"

        cls.cleanup()
        cls.create_test_db()

    @classmethod 
    def create_test_db(cls):
        
        cls.spark.sql(f"CREATE DATABASE {cls.tenant_name}")
        
        cls.spark.sql(f"""CREATE TABLE {cls.tenant_name}.bronze_totalmobile_bulletin (
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
        
        cls.spark.sql(f"""CREATE TABLE {cls.tenant_name}.silver_{cls.domain_name}_bulletin (
                                unique_key STRING NOT NULL,
                                category STRING,
                                subject STRING,
                                status INT,
                                user_key STRING,
                                user_name STRING,
                                message_time TIMESTAMP,
                                load_date TIMESTAMP
                            )
                            USING DELTA
                            TBLPROPERTIES (
                                'delta.enableChangeDataFeed' = 'true'
                            );""")
        
        cls.spark.sql(f"""CREATE TABLE IF NOT EXISTS {cls.tenant_name}.gold_{cls.domain_name}_dim_date (
                                id BIGINT GENERATED ALWAYS AS IDENTITY,
                                calendar_date DATE,
                                year INT,
                                month INT,
                                month_name STRING,
                                month_name_short CHAR(3),
                                day_of_month INT,
                                day_of_week INT,
                                day_name STRING,
                                day_of_year INT,
                                week_of_year INT,
                                quarter_of_year INT,
                                last_day_of_month CHAR(1),
                                year_week STRING,
                                year_month STRING,
                                load_date TIMESTAMP
                            )
                            USING DELTA
                            TBLPROPERTIES (
                                'delta.enableChangeDataFeed' = 'true'
                            )""")

        cls.spark.sql(f"""CREATE TABLE IF NOT EXISTS {cls.tenant_name}.gold_{cls.domain_name}_dim_user (
                                id BIGINT GENERATED ALWAYS AS IDENTITY,
                                user_key STRING,
                                user_name STRING,
                                deleted STRING,
                                load_date TIMESTAMP
                            )
                            USING DELTA
                            TBLPROPERTIES (
                                'delta.enableChangeDataFeed' = 'true'
                            )""")

        cls.spark.sql(f"""CREATE TABLE IF NOT EXISTS {cls.tenant_name}.gold_{cls.domain_name}_dim_subject (
                                id BIGINT GENERATED ALWAYS AS IDENTITY,
                                subject STRING,
                                load_date TIMESTAMP
                            )
                            USING DELTA
                            TBLPROPERTIES (
                                'delta.enableChangeDataFeed' = 'true'
                            )""")

        cls.spark.sql(f"""CREATE TABLE IF NOT EXISTS {cls.tenant_name}.gold_{cls.domain_name}_dim_status (
                                id BIGINT GENERATED ALWAYS AS IDENTITY,
                                status INT,
                                status_description STRING,
                                load_date TIMESTAMP
                            )
                            USING DELTA
                            TBLPROPERTIES (
                                'delta.enableChangeDataFeed' = 'true'
                            )""")

        cls.spark.sql(f"""CREATE TABLE IF NOT EXISTS {cls.tenant_name}.gold_{cls.domain_name}_fact_bulletins (
                                unique_key STRING NOT NULL,
                                dim_date_id BIGINT NOT NULL,
                                dim_user_id BIGINT NOT NULL,
                                dim_subject_id BIGINT NOT NULL,
                                dim_status_id BIGINT NOT NULL,
                                message_time TIMESTAMP,
                                category STRING,
                                load_date TIMESTAMP
                            )
                            USING DELTA
                            TBLPROPERTIES (
                                'delta.enableChangeDataFeed' = 'true'
                            )""")
        
        cls.spark.sql(f"""CREATE TABLE IF NOT EXISTS {cls.tenant_name}.bronze_totalmobile_dbo_TM_USER (
                                USR_GUID	STRING,
                                USR_ID	INT,
                                USR_USER_NAME	STRING,
                                USR_USER_KEY	STRING,
                                USR_DELETED	STRING,
                                USR_DELETED_TIMESTAMP	TIMESTAMP,
                                USR_DELETED_BY	STRING,
                                USR_ORIGINAL_USER_NAME	STRING,
                                USR_TYPE	STRING
                            )
                            USING DELTA
                            LOCATION '/user/hive/warehouse/{cls.tenant_name}.db/bronze_totalmobile_dbo_TM_USER'
                            TBLPROPERTIES (
                                'delta.enableChangeDataFeed' = 'true'
                            )""")

        cls.spark.sql(f"""CREATE TABLE IF NOT EXISTS {cls.tenant_name}.bronze_totalmobile_dbo_TM_USER_DETAIL (
                               	UDT_GUID	STRING,
                                UDT_USR_ID	INT,
                                UDT_FORENAME	STRING,
                                UDT_SURNAME	STRING,
                                UDT_PHONE	STRING,
                                UDT_PHONE_MOBILE	STRING,
                                UDT_PHONE_WORK	STRING,
                                UDT_EMAIL	STRING,
                                UDT_EMAIL_WORK	STRING,
                                UDT_JOB_TITLE	STRING,
                                UDT_ADDRESS	STRING,
                                UDT_PICTURE_FILE_ID	STRING
                            )
                            USING DELTA
                            LOCATION '/user/hive/warehouse/{cls.tenant_name}.db/bronze_totalmobile_dbo_TM_USER_DETAIL'
                            TBLPROPERTIES (
                                'delta.enableChangeDataFeed' = 'true'
                            )""")

    @classmethod
    def drop_test_db(cls):
        cls.spark.sql(f"DROP DATABASE IF EXISTS {cls.tenant_name} CASCADE")
    
    def test_write_stream_for_silver_bulletin(self):
        table_name = "bulletin"
        data_layer = "silver"
        primary_keys = None

        repository = new_repository()

        repository._build_table_paths = mock_build_table_paths

        interactor = new_interactor(repository=repository)
        processor = new_processor(interactor=interactor)

        processor.handler(self.tenant_name, self.domain_name, table_name, primary_keys, data_layer)

        self.spark.sql(f"""INSERT INTO {self.tenant_name}.bronze_totalmobile_bulletin 
            (unique_key, category, subject, event_time, status, recipients, load_date) 
            VALUES ('0', 'category0', 'subject0', '2021-01-01 00:00:00', 0, ARRAY(STRUCT('a0', 'b0', ARRAY(MAP('key1', 'value1', 'key2', 'value2')))),'2021-01-01 00:00:00')""")

        self.spark.sql(f"""INSERT INTO {self.tenant_name}.bronze_totalmobile_bulletin 
            (unique_key, category, subject, event_time, status, recipients, load_date) 
            VALUES ('1', 'category1', 'subject1', '2021-01-01 00:00:00', 1, ARRAY(STRUCT('a1', 'b1', ARRAY(MAP('key1', 'value1', 'key2', 'value2')))),'2021-01-01 00:00:00')""")
        
        self.spark.sql(f"""INSERT INTO {self.tenant_name}.bronze_totalmobile_bulletin 
            (unique_key, category, subject, event_time, status, recipients, load_date) 
            VALUES ('2', 'category2', 'subject2', '2021-01-01 00:00:00', 2, ARRAY(STRUCT('a2', 'b2', ARRAY(MAP('key1', 'value1', 'key2', 'value2')))),'2021-01-01 00:00:00')""")
        
        time.sleep(10)
        interactor.active_streaming_query.stop()

        sink_df = self.spark.sql(f"""select * 
                                     from {self.tenant_name}.silver_{self.domain_name}_bulletin
                                     where unique_key in (0,1,2)""")
        records_count = sink_df.count()

        self.assertEqual(records_count, 2, "Records count is not equal to 2")

    def test_write_batch_for_gold_dim_date(self):
        table_name = "dim_date"
        data_layer = "gold"
        primary_keys = "calendar_date"

        repository = new_repository()

        repository._build_table_paths = mock_build_table_paths

        interactor = new_interactor(repository=repository)
        processor = new_processor(interactor=interactor)

        processor.handler(self.tenant_name, self.domain_name, table_name, primary_keys, data_layer)

        sink_df = self.spark.sql(f"""select * 
                                 from {self.tenant_name}.{data_layer}_{self.domain_name}_{table_name}
                                 where calendar_date > current_date()""")
        records_count = sink_df.count()

        self.assertEqual(records_count, 1825, "Records count is not equal to 1825")

    def test_write_stream_for_gold_dim_user_table(self):
        table_name = "dim_user"
        data_layer = "gold"
        primary_keys = "user_key"

        repository = new_repository()

        repository._build_table_paths = mock_build_table_paths

        interactor = new_interactor(repository=repository)
        processor = new_processor(interactor=interactor)

        processor.handler(self.tenant_name, self.domain_name, table_name, primary_keys, data_layer)

        time.sleep(10)

        self.spark.sql(f"""insert into {self.tenant_name}.bronze_totalmobile_dbo_TM_USER
                            values ('6905ba10-630f-ea11-add2-2818782a2aa4', 145, 'gerard.murray', 'bdf6d05b-87eb-4daa-8ad3-2ff781325463', 'N', null, null, null, 'User')""")
        
        self.spark.sql(f"""insert into {self.tenant_name}.bronze_totalmobile_dbo_TM_USER 
                            values ('7105ba10-630f-ea11-add2-2818782a2aa4', 146, 'gerard.black', 'adf6d05b-87eb-4daa-8ad3-2ff781325463', 'Y', null, null, null, 'User')""")
        
        self.spark.sql(f"""insert into {self.tenant_name}.bronze_totalmobile_dbo_TM_USER 
                            values ('8205ba10-630f-ea11-add2-2818782a2aa4', 147, 'john.black', 'cef6d05b-87eb-4daa-8ad3-2ff781325463', 'Y', null, null, null, 'User')""")
  
        self.spark.sql(f"""insert into {self.tenant_name}.bronze_totalmobile_dbo_TM_USER_DETAIL 
                            values ('6905ba10-630f-ea11-add2-2818782a2aa4', 145, 'gerard', 'murray', '432', '5422', '5454', 'gerard@gmail.com', null, 'PS', null, null)""")
        
        self.spark.sql(f"""insert into {self.tenant_name}.bronze_totalmobile_dbo_TM_USER_DETAIL 
                    values ('7105ba10-630f-ea11-add2-2818782a2aa4', 146, 'gerard', 'black', '3432', '45422', '35454', 'black@gmail.com', null, 'PS', null, null)""")
        
        time.sleep(120)
        interactor.active_streaming_query.stop()

        sink_df = self.spark.sql(f"select * from {self.tenant_name}.gold_{self.domain_name}_dim_user")
        records_count = sink_df.count()

        self.assertEqual(records_count, 2, "Records count is not equal to 2")

    def test_write_stream_for_gold_dim_subject_table(self):
        table_name = "dim_subject"
        data_layer = "gold"
        primary_keys = "subject"

        repository = new_repository()
        repository._build_table_paths = mock_build_table_paths

        interactor = new_interactor(repository=repository)
        processor = new_processor(interactor=interactor)

        processor.handler(self.tenant_name, self.domain_name, table_name, primary_keys, data_layer)

        time.sleep(5)

        self.spark.sql(f"""insert into {self.tenant_name}.silver_care_bulletin
                            values ('unique_key1', 'Bulletin', 'Subject1', 0, 'user_key1', 'user_name1', current_timestamp(), current_timestamp())""")
        
        self.spark.sql(f"""insert into {self.tenant_name}.silver_care_bulletin 
                            values ('unique_key2', 'Bulletin', 'Subject2', 0, 'user_key2', 'user_name2', current_timestamp(), current_timestamp())""")
        
        self.spark.sql(f"""insert into {self.tenant_name}.silver_care_bulletin 
                            values ('unique_key3', 'Bulletin', 'Subject1', 0, 'user_key3', 'user_name3', current_timestamp(), current_timestamp())""")
        
        time.sleep(20)
        interactor.active_streaming_query.stop()

        sink_df = self.spark.sql(f"select * from {self.tenant_name}.gold_{self.domain_name}_dim_subject")
        records_count = sink_df.count()

        self.assertEqual(records_count, 2, "Records count is not equal to 2")

    def test_write_stream_for_gold_dim_status_table(self):
        table_name = "dim_status"
        data_layer = "gold"
        primary_keys = "status"

        repository = new_repository()
        repository._build_table_paths = mock_build_table_paths

        interactor = new_interactor(repository=repository)
        processor = new_processor(interactor=interactor)

        processor.handler(self.tenant_name, self.domain_name, table_name, primary_keys, data_layer)
        
        time.sleep(5)

        self.spark.sql(f"""insert into {self.tenant_name}.silver_care_bulletin
                            values ('unique_key1', 'Bulletin', 'Subject1', 0, 'user_key1', 'user_name1', current_timestamp(), current_timestamp())""")
        
        self.spark.sql(f"""insert into {self.tenant_name}.silver_care_bulletin 
                            values ('unique_key2', 'Bulletin', 'Subject2', 1, 'user_key2', 'user_name2', current_timestamp(), current_timestamp())""")
        
        self.spark.sql(f"""insert into {self.tenant_name}.silver_care_bulletin 
                            values ('unique_key3', 'Bulletin', 'Subject1', 1, 'user_key3', 'user_name3', current_timestamp(), current_timestamp())""")
        
        time.sleep(20)
        interactor.active_streaming_query.stop()

        sink_df = self.spark.sql(f"select * from {self.tenant_name}.gold_{self.domain_name}_dim_status")
        records_count = sink_df.count()

        self.assertEqual(records_count, 2, "Records count is not equal to 2")

    def test_write_stream_for_gold_fact_bulletin_table(self):
        table_name = "fact_bulletins"
        data_layer = "gold"
        primary_keys = "unique_key, dim_user_id"

        repository = new_repository()
        repository._build_table_paths = mock_build_table_paths

        interactor = new_interactor(repository=repository)
        processor = new_processor(interactor=interactor)

        processor.handler(self.tenant_name, self.domain_name, table_name, primary_keys, data_layer)

        time.sleep(10)

        # bulletin data
        self.spark.sql(f"""insert into {self.tenant_name}.silver_care_bulletin
                            values ('unique_key1', 'Bulletin', 'Subject_01', 2, 'user_key_01', 'user_name1', current_timestamp(), current_timestamp())""")
        self.spark.sql(f"""insert into {self.tenant_name}.silver_care_bulletin 
                            values ('unique_key2', 'Bulletin', 'Subject_02', 3, 'user_key_02', 'user_name2', current_timestamp(), current_timestamp())""")
        self.spark.sql(f"""insert into {self.tenant_name}.silver_care_bulletin 
                            values ('unique_key3', 'Bulletin', 'Subject_01', 3, 'user_key_03', 'user_name3', current_timestamp(), current_timestamp())""")
        self.spark.sql(f"""insert into {self.tenant_name}.silver_care_bulletin 
                            values ('unique_key4', 'Bulletin', 'Subject_03', 3, 'user_key_01', 'user_name1', current_timestamp(), current_timestamp())""")
        # subject data
        self.spark.sql(f"""insert into {self.tenant_name}.gold_care_dim_subject (subject, load_date)
                    values ('Subject_01', current_timestamp())""")
        self.spark.sql(f"""insert into {self.tenant_name}.gold_care_dim_subject (subject, load_date)
                    values ('Subject_02', current_timestamp())""")
        
        # user data
        self.spark.sql(f"""insert into {self.tenant_name}.gold_care_dim_user (user_key, user_name, deleted, load_date)
                    values ('user_key_01', 'user_name1', 'Y', current_timestamp())""")
        self.spark.sql(f"""insert into {self.tenant_name}.gold_care_dim_user  (user_key, user_name, deleted, load_date)
                    values ('user_key_02', 'user_name2', 'N', current_timestamp())""")
        
        # status data
        self.spark.sql(f"""insert into {self.tenant_name}.gold_care_dim_status (status, status_description, load_date)
                    values (2, 'DONE',  current_timestamp())""")
        self.spark.sql(f"""insert into {self.tenant_name}.gold_care_dim_status  (status, status_description, load_date)
                    values (3, 'DELETED', current_timestamp())""")

        time.sleep(120)
        interactor.active_streaming_query.stop()

        sink_df = self.spark.sql(f"""select * 
                                 from {self.tenant_name}.gold_{self.domain_name}_{table_name}
                                 where unique_key in ('unique_key1','unique_key2','unique_key3','unique_key4')""")
        records_count = sink_df.count()

        self.assertEqual(records_count, 2, "Records count is not equal to 2")

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