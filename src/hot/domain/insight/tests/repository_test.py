from typing import Optional, Callable
import logging
import pytest
from pipeline.storage.repository import Repository
from pipeline.usecase.interactor import Interactor
from pipeline.tests.testutils.mock_spark import MockDataStreamReader, MockDataStreamWriter, MockSparkSession, MockDataFrame
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(levelname)s | %(message)s", datefmt="%d/%m/%Y %I:%M:%S", level=logging.INFO)


class TestRepository():

    @pytest.fixture()
    def spark(self):
        builder: SparkSession.Builder = SparkSession.builder
        spark: SparkSession = builder.master("local").appName("UnitTests").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        return spark


class TestRespository_read_stream(TestRepository):
    def test_happy_path(self, spark):

        expected_load_res_data = [("1", "111"),("2", "222"),("3", "333")]

        expected_load_res = spark.createDataFrame(
            data=expected_load_res_data, 
            schema=["ref", "resourceRef"]
        )

        tenant_name = "tester"
        product_name = "unittests"
        data_layer = "bronze"
        source_table_name = "sourcetable"

        def format_func(source):
            assert source == "delta"

        def load_func(path):
            assert path == f"/mnt/{tenant_name}/lakehouse/{data_layer}/{tenant_name}.{data_layer}_{product_name}_{source_table_name}"
            return expected_load_res 
        
        mock_spark_reader = MockDataStreamReader(
            format_func=format_func,
            load_func=load_func
        )

        mock_spark = MockSparkSession(dataStreamReader=mock_spark_reader)

        r = Repository(spark_sess=mock_spark, delta=None)

        res_df = r.read_stream(
            source_table_name=source_table_name, 
            data_layer=data_layer, 
            tenant_name=tenant_name, 
            product_name=product_name
        )


class TestRepository_write_stream_shift(TestRepository):
    def test_happy_path(self, spark):

        tenant_name = "tester"
        product_name = "unittests"

        def format_func(source):
            assert source == "delta"

        def outputMode_func(outputMode):
            assert outputMode == "update"

        def option_func(key, value):
            assert key == "checkpointLocation"
            assert value == f"/mnt/{tenant_name}/lakehouse/silver_checkpoint/{tenant_name}.silver_{product_name}_shift"
    
        def queryName_func(queryName):
            assert queryName == "shift-silver-stream"

        def trigger_func(availableNow):
            assert availableNow == True

        def foreachBatch_func(func: Callable[[DataFrame, int], None]):
            assert func is not None
        
        def start_func(path):
            assert path == f"/mnt/{tenant_name}/lakehouse/silver/{tenant_name}.silver_{product_name}_shift"

        mock_data_stream_writer = MockDataStreamWriter(
            format_func=format_func,
            option_func=option_func,
            outputMode_func=outputMode_func,
            queryName_func=queryName_func,
            trigger_func=trigger_func,
            foreachBatch_func=foreachBatch_func,
            start_func=start_func,
        )

        df = MockDataFrame(
            dataStreamWriter=mock_data_stream_writer
        )

        r = Repository(spark_sess=None, delta=None)

        res = r.write_stream_shift(
            df=df,
            tenant_name=tenant_name, 
            product_name=product_name,
            schema=None
        )

        assert res is None


if __name__ == "__main__":
    pytest.main()