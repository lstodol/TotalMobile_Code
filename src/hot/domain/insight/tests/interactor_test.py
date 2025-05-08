import pytest
import logging
from datetime import datetime
from pipeline.usecase.interactor import Interactor
from pipeline.storage.repository import RepositoryInterface
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StringType, TimestampType, StructField


logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(levelname)s | %(message)s", datefmt="%d/%m/%Y %I:%M:%S", level=logging.INFO)

class TestInteractor():

    class MockRepository(RepositoryInterface):
        def __init__(self, read_stream_func, write_stream_shift_func) -> None:
            # method call counts
            self.read_stream_count = 0
            self.write_stream_shift_count = 0

            # method overrides
            self._read_stream_func = read_stream_func
            self._write_stream_shift_func = write_stream_shift_func

        def read_stream(self, source_table_name, data_layer, tenant_name, product_name) -> DataFrame:
            self.read_stream_count += 1
            return self._read_stream_func(source_table_name, data_layer, tenant_name, product_name)

        def write_stream_shift(self, df, tenant_name, product_name, schema):
            self.write_stream_shift_count += 1
            return self._write_stream_shift_func(df, tenant_name, product_name, schema)
        

    @pytest.fixture()
    def spark(self):
        builder: SparkSession.Builder = SparkSession.builder
        spark = builder.appName("Interactor Unit Tests").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        return spark
    

class TestInteractorShift(TestInteractor):
    def test_shift_loading_happy_path(self, spark):
        stream_schema = StructType([
            StructField(name="shift_ref", dataType=StringType(), nullable=False),
            StructField(name="resource_ref", dataType=StringType(), nullable=False),
            StructField(name="start_datetime_local", dataType=TimestampType(), nullable=False),
            StructField(name="timezone", dataType=StringType(), nullable=False)
        ])
        stream_data = [("12345", "1", datetime(2024, 4, 17, 12, 0), "GMT")]

        tenant_name_arg = "tester"
        product_name_arg = "unittests"

        def read_stream(source_table_name, data_layer, tenant_name, product_name) -> DataFrame:
            assert tenant_name == tenant_name_arg
            assert product_name == product_name_arg
            return spark.createDataFrame(data=stream_data, schema=stream_schema)

        def write_stream_shift(df, tenant_name, product_name, schema):
            assert type(df) == DataFrame
            assert df.schema.fieldNames() == schema.fieldNames()
            assert df.count() == 1
            assert tenant_name == tenant_name_arg
            assert product_name == product_name_arg

        r = self.MockRepository(read_stream_func=read_stream, write_stream_shift_func=write_stream_shift)

        i = Interactor(repository=r)

        i.load_silver_shift_table(tenant_name=tenant_name_arg, product_name=product_name_arg)

        assert r.read_stream_count == 1
        assert r.write_stream_shift_count == 1


    def test_handles_empty_read_stream(self, spark):
        stream_schema = StructType([
            StructField(name="shift_ref", dataType=StringType(), nullable=False),
            StructField(name="resource_ref", dataType=StringType(), nullable=False),
            StructField(name="start_datetime_local", dataType=TimestampType(), nullable=False),
            StructField(name="timezone", dataType=StringType(), nullable=False)
        ])
        stream_data = []

        tenant_name_arg = "tester"
        product_name_arg = "unittests"

        def read_stream(source_table_name, data_layer, tenant_name, product_name) -> DataFrame:
            assert tenant_name == tenant_name_arg
            assert product_name == product_name_arg
            return spark.createDataFrame(data=stream_data, schema=stream_schema)

        def write_stream_shift(df, tenant_name, product_name, schema):
            assert type(df) == DataFrame
            assert df.schema.fieldNames() == schema.fieldNames()
            assert df.count() == 0
            assert tenant_name == tenant_name_arg
            assert product_name == product_name_arg

        r = self.MockRepository(read_stream_func=read_stream, write_stream_shift_func=write_stream_shift)

        i = Interactor(repository=r)

        i.load_silver_shift_table(tenant_name=tenant_name_arg, product_name=product_name_arg)

        assert r.read_stream_count == 1
        assert r.write_stream_shift_count == 1

if __name__ == "__main__":
    pytest.main(verbosity=2)
