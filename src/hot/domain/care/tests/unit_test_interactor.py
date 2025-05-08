import unittest
from unittest.mock import Mock, call
import fixpath
from hot.domain.care.usecase.interactor import Interactor, new_interactor
from hot.domain.care.storage.repository_interface import RepositoryInterface
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, StructType, TimestampType


class TestDomainCareInteractor(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        #cls.spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        cls.spark = SparkSession.getActiveSession()

    def setUp(self):
        self.repository_mock = Mock(spec=RepositoryInterface)
        self.interactor = new_interactor(self.repository_mock)
        self.interactor.spark = self.spark

        # Set up mocks for repository methods
        self.repository_mock.read_stream = Mock()
        self.repository_mock.write_silver_stream = Mock()
        self.repository_mock.write_gold_batch = Mock()
        self.repository_mock.write_gold_stream = Mock()
        
        # Define HOT bulletin schemas
        self.user_schema = StructType([
            StructField("usr_id", IntegerType(), True),
            StructField("usr_user_key", StringType(), True),
            StructField("usr_deleted", StringType(), True)
        ])

        self.user_detail_schema = StructType([
            StructField("udt_usr_id", IntegerType(), True),
            StructField("udt_forename", StringType(), True),
            StructField("udt_surname", StringType(), True)
        ])

        self.bulletin_schema = StructType([
            StructField("unique_key", StringType(), True),
            StructField("category", StringType(), True),
            StructField("subject", StringType(), True),
            StructField("event_time", TimestampType(), True),
            StructField("status", IntegerType(), True),
            StructField("recipients", ArrayType(StructType([
                StructField("user_key", StringType(), True),
                StructField("name", StringType(), True)
            ])), True)
        ])

        self.subject_schema = StructType([
            StructField("subject", StringType(), True)
        ])

        self.status_schema = StructType([
            StructField("status", IntegerType(), True)
        ])

        self.fact_bulletin_schema = StructType([
            StructField("unique_key", StringType(), True),
            StructField("user_key", StringType(), True),
            StructField("message_time", TimestampType(), True),
            StructField("category", StringType(), True),
            StructField("subject", StringType(), True),
            StructField("status", IntegerType(), True)
        ])

        self.date_dim_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("calendar_date", TimestampType(), True)
        ])

        self.user_dim_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("user_key", StringType(), True),
            StructField("user_name", StringType(), True)
        ])

        self.subject_dim_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("subject", StringType(), True)
        ])

        self.status_dim_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("status", IntegerType(), True)
        ])

    def test_load_silver_care_bulletin_table(self):
        tenant_name = "tenant_name"
        domain_name = "domain_name"
        table_name = "table_name"
        primary_keys = "primary_keys"
        data_layer = "silver"

        mock_df = self.spark.createDataFrame([], schema=self.bulletin_schema)
        self.repository_mock.read_stream.return_value = mock_df

        self.interactor.load_silver_care_bulletin_table(tenant_name, domain_name, table_name, primary_keys, data_layer)

        self.repository_mock.write_silver_stream.assert_called_once()
        self.repository_mock.read_stream.assert_called_once_with(tenant_name, "bronze", "bronze_totalmobile_bulletin")

    def test_load_gold_care_dim_date_table(self):
        tenant_name = "tenant_name"
        domain_name = "domain_name"
        table_name = "table_name"
        primary_keys = "primary_keys"
        data_layer = "gold"

        self.interactor.load_gold_care_dim_date_table(tenant_name, domain_name, table_name, primary_keys, data_layer)

        self.repository_mock.write_gold_batch.assert_called_once()
        args, kwargs = self.repository_mock.write_gold_batch.call_args
        
        #merge_statement = """target_df.alias("tgt").merge(df.alias("src"), self._get_composite_equal_sql("src", "tgt", primary_keys)).withSchemaEvolution().whenNotMatchedInsert(values = {"tgt.calendar_date": "src.calendar_date", "tgt.year": "src.year", "tgt.month": "src.month", "tgt.month_name": "src.month_name", "tgt.month_name_short": "src.month_name_short", "tgt.day_of_month": "src.day_of_month", "tgt.day_of_week": "src.day_of_week", "tgt.day_name": "src.day_name", "tgt.day_of_year": "src.day_of_year", "tgt.week_of_year": "src.week_of_year", "tgt.quarter_of_year": "src.quarter_of_year", "tgt.last_day_of_month": "src.last_day_of_month", "tgt.year_week": "src.year_week", "tgt.year_month": "src.year_month", "tgt.load_date": "src.load_date"}).execute()"""
        self.assertIn("merge_statement", kwargs)

    def test_load_gold_care_dim_user_table(self):
        tenant_name = "tenant_name"
        domain_name = "domain_name"
        table_name = "table_name"
        primary_keys = "primary_keys"
        data_layer = "gold"

        mock_user_df = self.spark.createDataFrame([], schema=self.user_schema)
        mock_user_detail_df = self.spark.createDataFrame([], schema=self.user_detail_schema)
        self.repository_mock.read_stream.side_effect = [mock_user_df, mock_user_detail_df]

        self.interactor.load_gold_care_dim_user_table(tenant_name, domain_name, table_name, primary_keys, data_layer)

        self.repository_mock.write_gold_stream.assert_called_once()
        calls = [
            call(tenant_name, "bronze", "bronze_totalmobile_dbo_TM_USER"),
            call(tenant_name, "bronze", "bronze_totalmobile_dbo_TM_USER_DETAIL")
        ]
        self.repository_mock.read_stream.assert_has_calls(calls)

    def test_load_gold_care_dim_subject_table(self):
        tenant_name = "tenant_name"
        domain_name = "domain_name"
        table_name = "table_name"
        primary_keys = "primary_keys"
        data_layer = "gold"

        mock_df = self.spark.createDataFrame([], schema=self.subject_schema)
        self.repository_mock.read_stream.return_value = mock_df

        self.interactor.load_gold_care_dim_subject_table(tenant_name, domain_name, table_name, primary_keys, data_layer)

        self.repository_mock.write_gold_stream.assert_called_once()
        self.repository_mock.read_stream.assert_called_once_with(tenant_name, "silver", "silver_care_bulletin")

    def test_load_gold_care_dim_status_table(self):
        tenant_name = "tenant_name"
        domain_name = "domain_name"
        table_name = "table_name"
        primary_keys = "primary_keys"
        data_layer = "gold"

        mock_df = self.spark.createDataFrame([], schema=self.status_schema)
        self.repository_mock.read_stream.return_value = mock_df

        self.interactor.load_gold_care_dim_status_table(tenant_name, domain_name, table_name, primary_keys, data_layer)

        self.repository_mock.write_gold_stream.assert_called_once()
        self.repository_mock.read_stream.assert_called_once_with(tenant_name, "silver", "silver_care_bulletin")

    def test_load_gold_care_fact_bulletins_table(self):
        tenant_name = "tenant_name"
        domain_name = "domain_name"
        table_name = "table_name"
        primary_keys = "primary_keys"
        data_layer = "gold"

        mock_bulletin_df = self.spark.createDataFrame([], schema=self.fact_bulletin_schema)
        mock_date_dim_df = self.spark.createDataFrame([], schema=self.date_dim_schema)
        mock_user_dim_df = self.spark.createDataFrame([], schema=self.user_dim_schema)
        mock_subject_dim_df = self.spark.createDataFrame([], schema=self.subject_dim_schema)
        mock_status_dim_df = self.spark.createDataFrame([], schema=self.status_dim_schema)
        self.repository_mock.read_stream.side_effect = [mock_bulletin_df, mock_date_dim_df, mock_user_dim_df, mock_subject_dim_df, mock_status_dim_df]

        self.interactor.load_gold_care_fact_bulletins_table(tenant_name, domain_name, table_name, primary_keys, data_layer)

        self.repository_mock.write_gold_stream.assert_called_once()
        calls = [
            call(tenant_name, "silver", "silver_care_bulletin"),
            call(tenant_name, "gold", "gold_care_dim_date"),
            call(tenant_name, "gold", "gold_care_dim_user"),
            call(tenant_name, "gold", "gold_care_dim_subject"),
            call(tenant_name, "gold", "gold_care_dim_status")
        ]
        self.repository_mock.read_stream.assert_has_calls(calls)

    def tearDown(self):
        """Teardown method called after every test method"""
        del self.repository_mock
        #del self.spark_mock
        del self.interactor

    @classmethod
    def tearDownClass(cls):
        """Teardown method called once after all test methods"""
        del cls.spark

if __name__ == '__main__':
    test_runner = unittest.main(argv=[''], exit=False)
    test_runner.result.printErrors()
