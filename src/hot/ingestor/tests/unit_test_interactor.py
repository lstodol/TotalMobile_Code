import pytest
import unittest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta

import fixpath
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, MapType
from hot.ingestor.usecase.interactor import Interactor, new_interactor


class TestIngestorInteractor(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Setup method called once before all test methods"""
        cls.spark = SparkSession.getActiveSession()
        
    def setUp(self):
        """Setup method called before every test method"""
        self.repository_mock = Mock()
        self.spark_mock = Mock()
        self.interactor = new_interactor(self.repository_mock, self.spark_mock)

        read_stream_schema = StructType([
                                StructField('body', StringType(), True), 
                                StructField('partition', StringType(), True), 
                                StructField('offset', StringType(), True), 
                                StructField('sequenceNumber', LongType(), True), 
                                StructField('enqueuedTime', TimestampType(), True), 
                                StructField('publisher', StringType(), True), 
                                StructField('partitionKey', StringType(), True), 
                                StructField('properties', MapType(StringType(), StringType(), True), True), 
                                StructField('systemProperties', MapType(StringType(), StringType(), True), True)])
        
        read_stream_data =  [
            ('{"unique_key":"TheKey1","category":"Category1"}', 0,	11640,	24,	datetime.strptime('2024-07-10 15:28:05', '%Y-%m-%d %H:%M:%S'), None, None,	{"tenant_id":"devtest1"},	{"x-opt-sequence-number-epoch":"-1"}),
            ('{"unique_key":"TheKey2","category":"Category2"}', 0,	12696,	27,	datetime.strptime('2024-07-10 16:49:04',  '%Y-%m-%d %H:%M:%S'), None, None,	{"tenant_id":"devtest2"},	{"x-opt-sequence-number-epoch":"-1"}),
            ('{"unique_key":"TheKey3","category":"Category3"}', 0,	12000,	25,	datetime.strptime('2024-07-10 15:30:34',  '%Y-%m-%d %H:%M:%S'), None, None,	{"tenant_id":"tenant_name"},	{"x-opt-sequence-number-epoch":"-1"})]

        self.test_df = self.spark.createDataFrame(data=read_stream_data, schema=read_stream_schema)

        self.entity_schema = StructType([StructField('unique_key', StringType(), True), StructField('category', StringType(), True)])


    def test_add_load_date(self):
        #act
        result_df = self.interactor._add_load_date(self.test_df)
        
        #assert
        self.assertIn('load_date', result_df.columns)
        self.assertEqual(result_df.schema['load_date'].dataType, TimestampType())
        self.assertGreater(result_df.first()['load_date'], datetime.now() - timedelta(minutes=1))


    def test_get_entity_message_schema(self):
        #act
        result_schema = self.interactor._get_entity_message_schema("tenant_name", "product_name", "entity_name")
        
        #assert
        self.spark_mock.sql.assert_called_once_with("SELECT * FROM tenant_name.bronze_product_name_entity_name LIMIT 1")

    @patch.object(Interactor, "_get_entity_message_schema", return_value=StructType([
        StructField('unique_key', StringType(), True), StructField('category', StringType(), True)]))
    def test_extract_and_filter_entity(self, mock_interactor):
        #act
        entity_df = self.interactor._extract_and_filter_entity(self.test_df, "tenant_name", "product_name", "entity_name")
        
        self.assertEqual(1, entity_df.count())
        self.assertEqual("TheKey3", entity_df.first()["unique_key"])

    def test_load_bronze_eventhub_entity(self):
        
        #arrange
        df = self.spark.createDataFrame(
                              [{"unique_key":"TheKey1","category":"Category1"}], 
                              StructType([StructField('unique_key', StringType(), True), StructField('category', StringType(), True)]))

        with patch.object(Interactor, "_extract_and_filter_entity", return_value= df) as mocked__extract_and_filter_entity:
            #act
            self.interactor.load_bronze_eventhub_entity("tenant_name", "product_name", "entity_name")

        self.repository_mock.read_stream.assert_called_once_with()
        mocked__extract_and_filter_entity.assert_called_once()
        self.repository_mock.write_stream.assert_called_once_with(
            df=df, 
            bronze_table_name='entity_name', 
            query_name='entity_name-eventhubs-stream', 
            tenant_name='tenant_name', 
            product_name='product_name')


    def tearDown(self):
        """Teardown method called after every test method"""
        del self.repository_mock
        del self.spark_mock
        del self.interactor

    @classmethod
    def tearDownClass(cls):
        """Teardown method called once after all test methods"""
        del cls.spark

if __name__ == '__main__':
    test_runner = unittest.main(argv=[''], exit=False)
    test_runner.result.printErrors()
