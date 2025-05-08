import logging
from modules.unity_logging import AzureBlobHandler, LoggerConfiguration
from modules.authorisation import Authorisation
from azure.storage.blob import BlobServiceClient
from unittest import TestCase
from unittest.mock import patch, call, Mock
import fixpath

class TestUnityLogging(TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Creating a custom logger for testing
        cls.custom_logger = logging.getLogger('test_logger')
        LoggerConfiguration.ROOT_LOGGER_INITIALISED = False

    def tearDown(self):
        LoggerConfiguration.ROOT_LOGGER_INITIALISED = False

    ### Test configuration for shared logs
    @patch.object(BlobServiceClient, "get_blob_client")
    @patch.object(Authorisation, "get_container_client", return_value = BlobServiceClient(account_url="https://testurl"))
    @patch.object(LoggerConfiguration, "_configure_logger")
    def test_configure_shared_tenant(self, mocked_configure_logger, mocked_get_container_client, mocked_get_blob_client):
        
        with self.assertLogs(None, level='INFO') as cm:

            # Creating an instance of LoggerConfiguration
            logger_config = LoggerConfiguration(tenant_name='shared', level=logging.INFO)

            # Calling the configure method
            logger_config.configure(self.custom_logger)

        # Assertions
        mocked_configure_logger.assert_called()
        self.assertIn("Main logger configured with params-> tenant_name: shared", cm.output[0])
        self.assertIn("Root logger configured with params-> tenant_name: shared", cm.output[1])
        self.assertEqual(LoggerConfiguration.ROOT_LOGGER_INITIALISED, True)

        # Assertions for AzureBlobHandler
        mocked_get_container_client.assert_called_once_with('logs')
        mocked_get_blob_client.assert_called_once()

        LoggerConfiguration.ROOT_LOGGER_INITIALISED = False
        
    ### Test configuration for tenant logs
    @patch.object(BlobServiceClient, "get_blob_client")
    @patch.object(Authorisation, "get_container_client", return_value = BlobServiceClient(account_url="https://testurl"))
    @patch.object(LoggerConfiguration, "_configure_logger")
    def test_configure_test_tenant(self, mocked_configure_logger, mocked_get_container_client, mocked_get_blob_client):
        
        with self.assertLogs(None, level='INFO') as cm:

            # Creating an instance of LoggerConfiguration
            logger_config = LoggerConfiguration(tenant_name='test_tenant', level=logging.INFO)

            # Calling the configure method
            logger_config.configure(self.custom_logger)

        # Assertions
        mocked_configure_logger.assert_called()
        self.assertIn("Main logger configured with params-> tenant_name: test_tenant", cm.output[0])
        self.assertIn("Root logger configured with params-> tenant_name: test_tenant", cm.output[1])
        self.assertEqual(LoggerConfiguration.ROOT_LOGGER_INITIALISED, True)

        # Assertions for AzureBlobHandler
        mocked_get_container_client.assert_called_once_with('test_tenant')
        mocked_get_blob_client.assert_called_once()