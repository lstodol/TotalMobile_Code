import unittest
from unittest.mock import Mock

import fixpath
from hot.ingestor.storage.repository import Repository, new_repository


class TestIngestorRepository(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Setup method called once before all test methods"""

    def setUp(self):
        """Setup method called before every test method"""
        self.spark_mock = Mock()
        self.repository = new_repository("fake connection", self.spark_mock)

    def test_read_stream(self):
        #arrange
        eh_conf = {
            "eventhubs.connectionString": 'pA5hMjJHHiVK8VXhk33x4A==',
            "eventhubs.startingPosition" : '{"offset": "0", "seqNo": -1, "enqueuedTime": null, "isInclusive": true}'
        }

        #act
        df = self.repository.read_stream()

        #assert
        self.spark_mock.readStream.format.assert_called_once_with("eventhubs")
        self.spark_mock.readStream.format().options.assert_called_once_with(**eh_conf)
        self.spark_mock.readStream.format().options().load.assert_called_once_with()


    def test_write_stream(self):

        #arrange
        df = Mock()
        expected_checkpoint = 'dbfs:/mnt/tenant_name/eventhub_bronze_checkpoint/tenant_name.bronze_product_name_bronze_table_name'
        expected_table = 'tenant_name.bronze_product_name_bronze_table_name'
        #act
        self.repository.write_stream(df, "bronze_table_name", "query_name", "tenant_name", "product_name")
        
        #assert
        df.writeStream.format.assert_called_once_with("delta")
        df.writeStream.format().outputMode.assert_called_once_with("append")
        df.writeStream.format().outputMode().queryName.assert_called_once_with("query_name")
        df.writeStream.format().outputMode().queryName().option.assert_called_once_with('checkpointLocation', expected_checkpoint)
        df.writeStream.format().outputMode().queryName().option().toTable.assert_called_once_with(expected_table)

    def tearDown(self):
        """Teardown method called after every test method"""
        del self.spark_mock 
        del self.repository

    @classmethod
    def tearDownClass(cls):
        """Teardown method called once after all test methods"""

if __name__ == '__main__':
    test_runner = unittest.main(argv=[''], exit=False)
    test_runner.result.printErrors()
