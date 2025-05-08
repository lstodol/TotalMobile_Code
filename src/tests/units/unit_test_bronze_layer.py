import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
)
from datetime import datetime
from modules.bronze_layer import IngestBronzeTable
from pyspark.dbutils import DBUtils
from delta.tables import DeltaTable


class TestBaseBronzeLayer(unittest.TestCase):
    tenant_name = "unittest"
    mount_location = "/mnt/unittest"
    product_name = "testproduct"
    schema_name = "dbo"
    table_name = "bronze_table_processing_unittest"
    primary_key = "Id"
    incremental_column = "validfrom"

    @classmethod
    def get_dbutils(cls):
        spark = SparkSession.builder.getOrCreate()
        return DBUtils(spark)

        cls.dbutils = DBUtils(spark)

    @classmethod
    def setUpClass(cls):
        print("Testing live table processing... Setup")

        cls.spark = SparkSession.builder.getOrCreate()

        print("Creating sample database")
        cls.spark.sql(
            f"CREATE DATABASE IF NOT EXISTS {cls.tenant_name} LOCATION '{cls.mount_location}'"
        )

        print("Creating delta table from sample dataframe")

        cls.entry_data = [
            (
                1,
                "John",
                "Smith",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                2,
                "Jennifer",
                "Smith",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                3,
                "Matthias",
                "Kotze",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
        ]

        cls.target_schema = StructType(
            fields=[
                StructField("Id", IntegerType(), True),
                StructField("Name", StringType(), True),
                StructField("Surname", StringType(), True),
                StructField("validfrom", TimestampType(), True),
                StructField("validto", TimestampType(), True),
                StructField("LoadDate", TimestampType(), True),
            ]
        )

        cls.temp_table_schema = StructType(
            fields=[
                StructField("Id", IntegerType(), True),
                StructField("Name", StringType(), True),
                StructField("Surname", StringType(), True),
                StructField("Flag", StringType(), True),
                StructField("validfrom", TimestampType(), True),
                StructField("validto", TimestampType(), True),
                StructField("LoadDate", TimestampType(), True),
            ]
        )

    def setUp(self):
        test_scenario = self._testMethodName.replace(
            "test_", ""
        )  # Ensure each test operates on separate target/temp table
        self.test_table_name = f"{self.tenant_name}.test_{test_scenario}_{self.product_name}_{self.schema_name}_{self.table_name}"

        self.target_table_name = f"{self.tenant_name}.bronze_{test_scenario}_{self.product_name}_{self.schema_name}_{self.table_name}"

        entry_df = self.spark.createDataFrame(self.entry_data, self.target_schema)
        entry_df.write.format("delta").mode("overwrite").option(
            "mergeSchema", "true"
        ).saveAsTable(self.target_table_name)

    @classmethod
    def tearDownClass(cls):
        print("Cleaning up after testing...")

        print(f"Dropping {cls.tenant_name} sample database.")
        cls.spark.sql(f"DROP DATABASE IF EXISTS {cls.tenant_name} CASCADE")

        print(f"Dropping all sample directories from {cls.mount_location}.")
        dbutils = cls.get_dbutils()
        dbutils.fs.rm(cls.mount_location, True)


class TestBronzeHistoryTableProcessing(TestBaseBronzeLayer):
    primary_key = "Id, Name"

    def test_process_table_history(self):
        print("Calling HISTORY table processing...")

        history_data = [
            (
                1,
                "John",
                "Smith",
                "I",
                datetime(2020, 11, 20, 10, 4, 19),
                datetime(2023, 11, 20, 10, 3, 19),
                datetime(2020, 11, 20, 10, 4, 19),
            ),
            (
                2,
                "Jennifer",
                "Brame",
                "I",
                datetime(2020, 11, 20, 10, 4, 19),
                datetime(2023, 11, 20, 10, 3, 19),
                datetime(2020, 11, 20, 10, 4, 19),
            ),
            (
                3,
                "Matthias",
                "Kotze",
                "I",
                datetime(2020, 11, 20, 10, 4, 19),
                datetime(2023, 11, 20, 10, 3, 19),
                datetime(2020, 11, 20, 10, 4, 19),
            ),
        ]
        history_df = self.spark.createDataFrame(history_data, self.temp_table_schema)

        ingest_bronze_table = IngestBronzeTable(
            TestBronzeHistoryTableProcessing.tenant_name,
            TestBronzeHistoryTableProcessing.product_name,
            TestBronzeHistoryTableProcessing.schema_name,
            TestBronzeHistoryTableProcessing.table_name,
            "HISTORY",
            TestBronzeHistoryTableProcessing.primary_key,
            '',
        )
        ingest_bronze_table.spark = self.spark

        ingest_bronze_table.process_table_history(
            target_df=DeltaTable.forName(
                ingest_bronze_table.spark, self.target_table_name
            ),
            source_df=history_df,
            primary_key=self.primary_key,
        )
        result_df = ingest_bronze_table.spark.table(self.target_table_name)

        expected_result_data = [
            (
                1,
                "John",
                "Smith",
                datetime(2020, 11, 20, 10, 4, 19),
                datetime(2023, 11, 20, 10, 3, 19),
                datetime(2020, 11, 20, 10, 4, 19),
            ),
            (
                2,
                "Jennifer",
                "Smith",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                3,
                "Matthias",
                "Kotze",
                datetime(2020, 11, 20, 10, 4, 19),
                datetime(2023, 11, 20, 10, 3, 19),
                datetime(2020, 11, 20, 10, 4, 19),
            ),
            (
                1,
                "John",
                "Smith",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                2,
                "Jennifer",
                "Brame",
                datetime(2020, 11, 20, 10, 4, 19),
                datetime(2023, 11, 20, 10, 3, 19),
                datetime(2020, 11, 20, 10, 4, 19),
            ),
            (
                3,
                "Matthias",
                "Kotze",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
        ]
        expected_result_df = self.spark.createDataFrame(
            expected_result_data, self.target_schema
        )

        self.assertEqual(
            sorted(result_df.collect()),
            sorted(expected_result_df.collect()),
            "Record mismatch after history load",
        )


class TestBronzeLiveTableDeletesProcessing(TestBaseBronzeLayer):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.entry_data = [
            (
                1,
                "John",
                "Smith",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                2,
                "Jennifer",
                "Brame",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                3,
                "Matthias",
                "Kotze",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
        ]

    def test_process_table_live_deletions(self):
        print("Calling LIVE table processing for deleted records...")

        deleted_data = [
            (
                3,
                "Matthias",
                "Kotze",
                "D",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            )
        ]
        deleted_df = self.spark.createDataFrame(deleted_data, self.temp_table_schema)

        ingest_bronze_table = IngestBronzeTable(
            TestBronzeLiveTableDeletesProcessing.tenant_name,
            TestBronzeLiveTableDeletesProcessing.product_name,
            TestBronzeLiveTableDeletesProcessing.schema_name,
            TestBronzeLiveTableDeletesProcessing.table_name,
            "LIVE",
            TestBronzeLiveTableDeletesProcessing.primary_key,
            TestBronzeLiveTableDeletesProcessing.incremental_column,
        )
        ingest_bronze_table.spark = self.spark

        ingest_bronze_table.process_table_live(
            target_df=DeltaTable.forName(
                ingest_bronze_table.spark, self.target_table_name
            ),
            source_df=deleted_df,
            primary_key=self.primary_key,
            incremental_column=self.incremental_column,
        )
        result_df = ingest_bronze_table.spark.table(self.target_table_name)

        expected_result_data = [
            (
                1,
                "John",
                "Smith",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                2,
                "Jennifer",
                "Brame",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
        ]
        expected_result_df = self.spark.createDataFrame(
            expected_result_data, self.target_schema
        )

        self.assertEqual(
            sorted(result_df.collect()),
            sorted(expected_result_df.collect()),
            "Record mismatch after delete operation",
        )


class TestBronzeLiveTableInsertsProcessing(TestBaseBronzeLayer):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.entry_data = [
            (
                1,
                "John",
                "Smith",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                2,
                "Jennifer",
                "Brame",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                3,
                "Matthias",
                "Kotze",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
        ]

    def test_process_table_live_inserts(self):
        print("Calling LIVE table processing for inserted records...")

        inserted_data = [
            (
                4,
                "Sven",
                "Wagner",
                "I",
                datetime(2023, 11, 21, 10, 4, 30),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 21, 10, 4, 30),
            ),
            (
                5,
                "Tim",
                "Carter",
                "I",
                datetime(2023, 11, 21, 10, 4, 30),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 21, 10, 4, 30),
            ),
        ]
        temp_df_inserts = self.spark.createDataFrame(
            inserted_data, self.temp_table_schema
        )

        ingest_bronze_table = IngestBronzeTable(
            TestBronzeLiveTableInsertsProcessing.tenant_name,
            TestBronzeLiveTableInsertsProcessing.product_name,
            TestBronzeLiveTableInsertsProcessing.schema_name,
            TestBronzeLiveTableInsertsProcessing.table_name,
            "LIVE",
            TestBronzeLiveTableInsertsProcessing.primary_key,
            TestBronzeLiveTableDeletesProcessing.incremental_column,
        )
        ingest_bronze_table.spark = self.spark
        ingest_bronze_table.process_table_live(
            target_df=DeltaTable.forName(
                ingest_bronze_table.spark, self.target_table_name
            ),
            source_df=temp_df_inserts,
            primary_key=self.primary_key,
            incremental_column=self.incremental_column,
        )
        result_df = ingest_bronze_table.spark.table(self.target_table_name)

        expected_result_data = [
            (
                1,
                "John",
                "Smith",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                2,
                "Jennifer",
                "Brame",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                3,
                "Matthias",
                "Kotze",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                4,
                "Sven",
                "Wagner",
                datetime(2023, 11, 21, 10, 4, 30),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 21, 10, 4, 30),
            ),
            (
                5,
                "Tim",
                "Carter",
                datetime(2023, 11, 21, 10, 4, 30),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 21, 10, 4, 30),
            ),
        ]
        expected_result_df = self.spark.createDataFrame(
            expected_result_data, self.target_schema
        )

        self.assertEqual(
            sorted(result_df.collect()),
            sorted(expected_result_df.collect()),
            "Record mismatch after insert operation",
        )


class TestBronzeLiveTableUpdatesProcessing(TestBaseBronzeLayer):
    primary_key = "Id, Name, Surname"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.entry_data = [
            (
                1,
                "John",
                "Smith",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                2,
                "Jennifer",
                "Brame",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                3,
                "Matthias",
                "Kotze",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
        ]

    def test_process_table_live_updates(self):
        print("Calling LIVE table processing for updated records...")

        updated_data = [
            (
                2,
                "Jennifer",
                "Brame",
                "D",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                2,
                "Jenny",
                "Brame",
                "I",
                datetime(2023, 11, 24, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 22, 10, 4, 19),
            ),
            (
                4,
                "Jenny",
                "Brame-Smith",
                "I",
                datetime(2023, 11, 24, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 22, 10, 4, 19),
            ),
        ]
        temp_df_updates = self.spark.createDataFrame(
            updated_data, self.temp_table_schema
        )

        ingest_bronze_table = IngestBronzeTable(
            TestBronzeLiveTableUpdatesProcessing.tenant_name,
            TestBronzeLiveTableUpdatesProcessing.product_name,
            TestBronzeLiveTableUpdatesProcessing.schema_name,
            TestBronzeLiveTableUpdatesProcessing.table_name,
            "LIVE",
            TestBronzeLiveTableUpdatesProcessing.primary_key,
            TestBronzeLiveTableDeletesProcessing.incremental_column,
        )
        ingest_bronze_table.spark = self.spark

        ingest_bronze_table.process_table_live(
            target_df=DeltaTable.forName(
                ingest_bronze_table.spark, self.target_table_name
            ),
            source_df=temp_df_updates,
            primary_key=self.primary_key,
            incremental_column=self.incremental_column,
        )
        result_df = ingest_bronze_table.spark.table(self.target_table_name)

        expected_result_data = [
            (
                1,
                "John",
                "Smith",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                2,
                "Jenny",
                "Brame",
                datetime(2023, 11, 24, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 22, 10, 4, 19),
            ),
            (
                3,
                "Matthias",
                "Kotze",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                4,
                "Jenny",
                "Brame-Smith",
                datetime(2023, 11, 24, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 22, 10, 4, 19),
            ),
        ]
        expected_result_df = self.spark.createDataFrame(
            expected_result_data, self.target_schema
        )

        self.assertEqual(
            sorted(result_df.collect()),
            sorted(expected_result_df.collect()),
            "Record mismatch after update operation",
        )


class TestBronzeReferenceTableProcessing(TestBaseBronzeLayer):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.entry_data = [
            (
                1,
                "John",
                "Smith",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                2,
                "Jennifer",
                "Brame",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                3,
                "Matthias",
                "Kotze",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
        ]

    def test_process_table_reference(self):
        print("Calling REFERENCE table processing...")

        reference_data = [
            (
                5,
                "Alice",
                "Johnson",
                "I",
                datetime(2023, 11, 22, 10, 4, 45),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 12, 1, 10, 4, 45),
            ),
            (
                6,
                "Bob",
                "Williams",
                "I",
                datetime(2023, 11, 22, 10, 4, 45),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 12, 1, 10, 4, 45),
            ),
        ]
        reference_df = self.spark.createDataFrame(
            reference_data, self.temp_table_schema
        )

        ingest_bronze_table = IngestBronzeTable(
            TestBronzeReferenceTableProcessing.tenant_name,
            TestBronzeReferenceTableProcessing.product_name,
            TestBronzeReferenceTableProcessing.schema_name,
            TestBronzeReferenceTableProcessing.table_name,
            "REFERENCE",
            TestBronzeReferenceTableProcessing.primary_key,
            '',
        )
        ingest_bronze_table.spark = self.spark

        ingest_bronze_table.process_table_reference(
            target_table_name=self.target_table_name,
            source_df=reference_df,
        )

        result_df = ingest_bronze_table.spark.table(self.target_table_name)

        expected_result_data = [
            (
                5,
                "Alice",
                "Johnson",
                datetime(2023, 11, 22, 10, 4, 45),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 12, 1, 10, 4, 45),
            ),
            (
                6,
                "Bob",
                "Williams",
                datetime(2023, 11, 22, 10, 4, 45),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 12, 1, 10, 4, 45),
            ),
        ]
        expected_result_df = self.spark.createDataFrame(
            expected_result_data, self.target_schema
        )

        self.assertEqual(
            sorted(result_df.collect()),
            sorted(expected_result_df.collect()),
            "Record mismatch after reference full load",
        )


#UpdateTimestamp test
class TestBronzeUpdateTimestampTableInsertsProcessing(TestBaseBronzeLayer):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.entry_data = [
            (
                1,
                "John",
                "Smith",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                2,
                "Jennifer",
                "Brame",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                3,
                "Matthias",
                "Kotze",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
        ]

    def test_process_table_UpdateTimestamp_inserts(self):
        print("Calling UpdateTimestamp table processing for inserted records...")

        inserted_data = [
            (
                4,
                "Sven",
                "Wagner",
                "IU",
                datetime(2023, 11, 21, 10, 4, 30),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 21, 10, 4, 30),
            ),
            (
                5,
                "Tim",
                "Carter",
                "IU",
                datetime(2023, 11, 21, 10, 4, 30),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 21, 10, 4, 30),
            ),
        ]
        temp_df_inserts = self.spark.createDataFrame(
            inserted_data, self.temp_table_schema
        )

        ingest_bronze_table = IngestBronzeTable(
            TestBronzeUpdateTimestampTableInsertsProcessing.tenant_name,
            TestBronzeUpdateTimestampTableInsertsProcessing.product_name,
            TestBronzeUpdateTimestampTableInsertsProcessing.schema_name,
            TestBronzeUpdateTimestampTableInsertsProcessing.table_name,
            "UPDATETIMESTAMP",
            TestBronzeUpdateTimestampTableInsertsProcessing.primary_key,
            TestBronzeUpdateTimestampTableInsertsProcessing.incremental_column,
        )
        ingest_bronze_table.spark = self.spark
        ingest_bronze_table.process_table_UpdateTimestamp(
            target_df=DeltaTable.forName(
                ingest_bronze_table.spark, self.target_table_name
            ),
            source_df=temp_df_inserts,
            primary_key=self.primary_key,
            incremental_column=self.incremental_column,
        )
        result_df = ingest_bronze_table.spark.table(self.target_table_name)

        expected_result_data = [
            (
                1,
                "John",
                "Smith",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                2,
                "Jennifer",
                "Brame",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                3,
                "Matthias",
                "Kotze",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                4,
                "Sven",
                "Wagner",
                datetime(2023, 11, 21, 10, 4, 30),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 21, 10, 4, 30),
            ),
            (
                5,
                "Tim",
                "Carter",
                datetime(2023, 11, 21, 10, 4, 30),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 21, 10, 4, 30),
            ),
        ]
        expected_result_df = self.spark.createDataFrame(
            expected_result_data, self.target_schema
        )

        self.assertEqual(
            sorted(result_df.collect()),
            sorted(expected_result_df.collect()),
            "Record mismatch after insert operation",
        )

class TestBronzeUpdateTimestampTableUpdatesProcessing(TestBaseBronzeLayer):
    primary_key = "Id"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.entry_data = [
            (
                1,
                "John",
                "Smith",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                2,
                "Jennifer",
                "Brame",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                3,
                "Matthias",
                "Kotze",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
        ]

    def test_process_table_UpdateTimestamp_updates(self):
        print("Calling UpdateTimestamp table processing for updated records...")

        updated_data = [
            (
                2,
                "Jenny",
                "Brame",
                "IU",
                datetime(2023, 11, 24, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 22, 10, 4, 19),
            ),
            (
                4,
                "Jenny",
                "Brame-Smith",
                "IU",
                datetime(2023, 11, 24, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 22, 10, 4, 19),
            ),
        ]
        temp_df_updates = self.spark.createDataFrame(
            updated_data, self.temp_table_schema
        )

        ingest_bronze_table = IngestBronzeTable(
            TestBronzeUpdateTimestampTableUpdatesProcessing.tenant_name,
            TestBronzeUpdateTimestampTableUpdatesProcessing.product_name,
            TestBronzeUpdateTimestampTableUpdatesProcessing.schema_name,
            TestBronzeUpdateTimestampTableUpdatesProcessing.table_name,
            "UPDATETIMESTAMP",
            TestBronzeUpdateTimestampTableUpdatesProcessing.primary_key,
            TestBronzeUpdateTimestampTableUpdatesProcessing.incremental_column,
        )
        ingest_bronze_table.spark = self.spark

        ingest_bronze_table.process_table_UpdateTimestamp(
            target_df=DeltaTable.forName(
                ingest_bronze_table.spark, self.target_table_name
            ),
            source_df=temp_df_updates,
            primary_key=self.primary_key,
            incremental_column=self.incremental_column,
        )
        result_df = ingest_bronze_table.spark.table(self.target_table_name)

        expected_result_data = [
            (
                1,
                "John",
                "Smith",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                2,
                "Jenny",
                "Brame",
                datetime(2023, 11, 24, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 22, 10, 4, 19),
            ),
            (
                3,
                "Matthias",
                "Kotze",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                4,
                "Jenny",
                "Brame-Smith",
                datetime(2023, 11, 24, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 22, 10, 4, 19),
            ),
        ]
        expected_result_df = self.spark.createDataFrame(
            expected_result_data, self.target_schema
        )

        self.assertEqual(
            sorted(result_df.collect()),
            sorted(expected_result_df.collect()),
            "Record mismatch after update operation",
        )
        
        
#ChangeTrackingVersion test

class TestBronzeChangeTrackingVersionTableDeletesProcessing(TestBaseBronzeLayer):
    primary_key = "Id"
    
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.entry_data = [
            (
                1,
                "John",
                "Smith",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                2,
                "Jennifer",
                "Brame",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                3,
                "Matthias",
                "Kotze",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
        ]

    def test_process_table_ChangeTrackingVersion_deletions(self):
        print("Calling ChangeTrackingVersion table processing for deleted records...")

        deleted_data = [
            (
                3,
                "Matthias",
                "Kotze",
                "D",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            )
        ]
        deleted_df = self.spark.createDataFrame(deleted_data, self.temp_table_schema)

        ingest_bronze_table = IngestBronzeTable(
            TestBronzeChangeTrackingVersionTableDeletesProcessing.tenant_name,
            TestBronzeChangeTrackingVersionTableDeletesProcessing.product_name,
            TestBronzeChangeTrackingVersionTableDeletesProcessing.schema_name,
            TestBronzeChangeTrackingVersionTableDeletesProcessing.table_name,
            "CHANGETRACKINGVERSION",
            TestBronzeChangeTrackingVersionTableDeletesProcessing.primary_key,
            TestBronzeChangeTrackingVersionTableDeletesProcessing.incremental_column,
        )
        ingest_bronze_table.spark = self.spark

        ingest_bronze_table.process_table_ChangeTrackingVersion(
            target_df=DeltaTable.forName(
                ingest_bronze_table.spark, self.target_table_name
            ),
            source_df=deleted_df,
            primary_key=self.primary_key,
            incremental_column=self.incremental_column,
        )
        result_df = ingest_bronze_table.spark.table(self.target_table_name)

        expected_result_data = [
            (
                1,
                "John",
                "Smith",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                2,
                "Jennifer",
                "Brame",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
        ]
        expected_result_df = self.spark.createDataFrame(
            expected_result_data, self.target_schema
        )

        self.assertEqual(
            sorted(result_df.collect()),
            sorted(expected_result_df.collect()),
            "Record mismatch after delete operation",
        )


class TestBronzeChangeTrackingVersionTableInsertsProcessing(TestBaseBronzeLayer):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.entry_data = [
            (
                1,
                "John",
                "Smith",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                2,
                "Jennifer",
                "Brame",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                3,
                "Matthias",
                "Kotze",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
        ]

    def test_process_table_ChangeTrackingVersion_inserts(self):
        print("Calling ChangeTrackingVersion table processing for inserted records...")

        inserted_data = [
            (
                4,
                "Sven",
                "Wagner",
                "IU",
                datetime(2023, 11, 21, 10, 4, 30),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 21, 10, 4, 30),
            ),
            (
                5,
                "Tim",
                "Carter",
                "IU",
                datetime(2023, 11, 21, 10, 4, 30),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 21, 10, 4, 30),
            ),
        ]
        temp_df_inserts = self.spark.createDataFrame(
            inserted_data, self.temp_table_schema
        )

        ingest_bronze_table = IngestBronzeTable(
            TestBronzeChangeTrackingVersionTableInsertsProcessing.tenant_name,
            TestBronzeChangeTrackingVersionTableInsertsProcessing.product_name,
            TestBronzeChangeTrackingVersionTableInsertsProcessing.schema_name,
            TestBronzeChangeTrackingVersionTableInsertsProcessing.table_name,
            "CHANGETRACKINGVERSION",
            TestBronzeChangeTrackingVersionTableInsertsProcessing.primary_key,
            TestBronzeChangeTrackingVersionTableInsertsProcessing.incremental_column,
        )
        ingest_bronze_table.spark = self.spark
        ingest_bronze_table.process_table_ChangeTrackingVersion(
            target_df=DeltaTable.forName(
                ingest_bronze_table.spark, self.target_table_name
            ),
            source_df=temp_df_inserts,
            primary_key=self.primary_key,
            incremental_column=self.incremental_column,
        )
        result_df = ingest_bronze_table.spark.table(self.target_table_name)

        expected_result_data = [
            (
                1,
                "John",
                "Smith",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                2,
                "Jennifer",
                "Brame",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                3,
                "Matthias",
                "Kotze",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                4,
                "Sven",
                "Wagner",
                datetime(2023, 11, 21, 10, 4, 30),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 21, 10, 4, 30),
            ),
            (
                5,
                "Tim",
                "Carter",
                datetime(2023, 11, 21, 10, 4, 30),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 21, 10, 4, 30),
            ),
        ]
        expected_result_df = self.spark.createDataFrame(
            expected_result_data, self.target_schema
        )

        self.assertEqual(
            sorted(result_df.collect()),
            sorted(expected_result_df.collect()),
            "Record mismatch after insert operation",
        )

class TestBronzeChangeTrackingVersionTableUpdatesProcessing(TestBaseBronzeLayer):
    primary_key = "Id"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.entry_data = [
            (
                1,
                "John",
                "Smith",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                2,
                "Jennifer",
                "Brame",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                3,
                "Matthias",
                "Kotze",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
        ]

    def test_process_table_ChangeTrackingVersion_updates(self):
        print("Calling ChangeTrackingVersion table processing for updated records...")

        updated_data = [
            (
                2,
                "Jenny",
                "Brame",
                "IU",
                datetime(2023, 11, 24, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 22, 10, 4, 19),
            ),
            (
                4,
                "Jenny",
                "Brame-Smith",
                "IU",
                datetime(2023, 11, 24, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 22, 10, 4, 19),
            ),
        ]
        temp_df_updates = self.spark.createDataFrame(
            updated_data, self.temp_table_schema
        )

        ingest_bronze_table = IngestBronzeTable(
            TestBronzeChangeTrackingVersionTableUpdatesProcessing.tenant_name,
            TestBronzeChangeTrackingVersionTableUpdatesProcessing.product_name,
            TestBronzeChangeTrackingVersionTableUpdatesProcessing.schema_name,
            TestBronzeChangeTrackingVersionTableUpdatesProcessing.table_name,
            "CHANGETRACKINGVERSION",
            TestBronzeChangeTrackingVersionTableUpdatesProcessing.primary_key,
            TestBronzeChangeTrackingVersionTableUpdatesProcessing.incremental_column,
        )
        ingest_bronze_table.spark = self.spark

        ingest_bronze_table.process_table_ChangeTrackingVersion(
            target_df=DeltaTable.forName(
                ingest_bronze_table.spark, self.target_table_name
            ),
            source_df=temp_df_updates,
            primary_key=self.primary_key,
            incremental_column=self.incremental_column,
        )
        result_df = ingest_bronze_table.spark.table(self.target_table_name)

        expected_result_data = [
            (
                1,
                "John",
                "Smith",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                2,
                "Jenny",
                "Brame",
                datetime(2023, 11, 24, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 22, 10, 4, 19),
            ),
            (
                3,
                "Matthias",
                "Kotze",
                datetime(2023, 11, 20, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 20, 10, 4, 19),
            ),
            (
                4,
                "Jenny",
                "Brame-Smith",
                datetime(2023, 11, 24, 10, 4, 19),
                datetime(9999, 12, 31, 23, 59, 59),
                datetime(2023, 11, 22, 10, 4, 19),
            ),
        ]
        expected_result_df = self.spark.createDataFrame(
            expected_result_data, self.target_schema
        )

        self.assertEqual(
            sorted(result_df.collect()),
            sorted(expected_result_df.collect()),
            "Record mismatch after update operation",
        )