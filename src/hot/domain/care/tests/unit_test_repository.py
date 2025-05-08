import unittest
import fixpath
from unittest.mock import MagicMock, patch, Mock
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery
from delta import DeltaTable
from hot.domain.care.storage.repository_interface import RepositoryInterface
from hot.domain.care.storage.repository import Repository, new_repository

class TestDomainCareRepository(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        #cls.spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        cls.spark = SparkSession.getActiveSession()
        
    def setUp(self):
        # Mock SparkSession and DeltaTable
        self.spark = MagicMock(SparkSession)
        self.delta = MagicMock(DeltaTable)

        # Instantiate the Repository with mocked dependencies
        self.repo = Repository(spark_sess=self.spark, delta=self.delta)

        # Mock DataFrame and StreamingQuery
        self.data_frame = MagicMock(DataFrame)
        self.streaming_query = MagicMock(StreamingQuery)

        # Mock paths
        self.table_path = "/mnt/tenant/lakehouse/data_layer/tenant.table_name"
        self.checkpoint_path = "/mnt/tenant/data_layer_checkpoint/tenant.table_name"


    def test_build_table_paths(self):
        tenant_name = "tenant"
        data_layer = "data_layer"
        table_name = "table_name"

        delta_table_path, delta_table_checkpoint_path = self.repo._build_table_paths(
            tenant_name, data_layer, table_name
        )

        expected_delta_table_path = "/mnt/tenant/lakehouse/data_layer/tenant.table_name"
        expected_delta_table_checkpoint_path = "/mnt/tenant/data_layer_checkpoint/tenant.table_name"

        self.assertEqual(delta_table_path, expected_delta_table_path)
        self.assertEqual(delta_table_checkpoint_path, expected_delta_table_checkpoint_path)

    def test_read_stream(self):
        tenant_name = "tenant"
        data_layer = "data_layer"
        source_table_name = "source_table_name"

        # Mock the internal _build_table_paths method
        with patch.object(self.repo, '_build_table_paths', return_value=(self.table_path, self.checkpoint_path)):
            result = self.repo.read_stream(tenant_name, data_layer, source_table_name)

        self.spark.readStream.format.assert_called_once_with("delta")
        self.spark.readStream.format().option.assert_called_once_with("ignoreChanges", "true")
        self.spark.readStream.format().option().table.assert_called_once_with(f"{tenant_name}.{source_table_name}")

    def test_write_silver_stream(self):
        tenant_name = "tenant"
        data_layer = "silver"
        target_table_name = "target_table_name"

        # Mock the internal _build_table_paths method
        with patch.object(self.repo, '_build_table_paths', return_value=(self.table_path, self.checkpoint_path)):
            result = self.repo.write_silver_stream(self.data_frame, target_table_name, data_layer, tenant_name)

        self.data_frame.writeStream.format.assert_called_once_with("delta")
        self.data_frame.writeStream.format().outputMode.assert_called_once_with("append")
        self.data_frame.writeStream.format().outputMode().option.assert_called_once_with("checkpointLocation", self.checkpoint_path)
        self.data_frame.writeStream.format().outputMode().option().trigger.assert_called_once_with(processingTime='5 seconds')
        self.data_frame.writeStream.format().outputMode().option().trigger().toTable.assert_called_once_with(f"{tenant_name}.{target_table_name}")

    def test_write_gold_batch(self):
        tenant_name = "tenant"
        data_layer = "gold"
        target_table_name = "target_table_name"
        primary_keys = "pk1,pk2"
        merge_statement = "target_df.alias('t').merge(df.alias('s'), 't.pk1 = s.pk1 AND t.pk2 = s.pk2').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()"

        # Mock the internal _build_table_paths method
        with patch.object(self.repo, '_build_table_paths', return_value=(self.table_path, self.checkpoint_path)):
            with patch("builtins.exec") as mock_exec:
                result = self.repo.write_gold_batch(self.data_frame, target_table_name, data_layer, tenant_name, primary_keys, merge_statement)

        self.delta.forPath.assert_called_once_with(self.spark, self.table_path)
        mock_exec.assert_called_once_with(merge_statement)

    def test_write_gold_stream(self):
        tenant_name = "tenant"
        data_layer = "gold"
        target_table_name = "target_table_name"
        primary_keys = "pk1,pk2"
        merge_statement = "target_df.alias('t').merge(df.alias('s'), 't.pk1 = s.pk1 AND t.pk2 = s.pk2').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()"

        # Mock the internal _build_table_paths method
        with patch.object(self.repo, '_build_table_paths', return_value=(self.table_path, self.checkpoint_path)):
            with patch("builtins.exec") as mock_exec:
                # Patch the DataFrame writeStream API to test foreachBatch
                with patch.object(self.data_frame.writeStream, 'foreachBatch', wraps=self.data_frame.writeStream.foreachBatch) as mock_foreachBatch:
                    result = self.repo.write_gold_stream(self.data_frame, target_table_name, data_layer, tenant_name, primary_keys, merge_statement)

                    self.data_frame.writeStream.format.assert_called_once_with("delta")
                    self.data_frame.writeStream.format().outputMode.assert_called_once_with("append")
                    self.data_frame.writeStream.format().outputMode().option.assert_called_once_with("checkpointLocation", self.checkpoint_path)
                    self.data_frame.writeStream.format().outputMode().option().trigger.assert_called_once_with(processingTime="5 seconds")
                    self.data_frame.writeStream.format().outputMode().option().trigger().foreachBatch.assert_called_once()
                    self.data_frame.writeStream.format().outputMode().option().trigger().foreachBatch().toTable.assert_called_once()

                    #Not able to test foreachBatch function, it needs to refectored to remove exec() statement


    def tearDown(self):
        """Teardown method called after every test method"""
        self.spark.reset_mock()
        self.delta.reset_mock()
        self.data_frame.reset_mock()
        self.streaming_query.reset_mock()

    @classmethod
    def tearDownClass(cls):
        """Teardown method called once after all test methods"""
        del cls.spark


if __name__ == '__main__':
    test_runner = unittest.main(argv=[''], exit=False)
    test_runner.result.printErrors()
