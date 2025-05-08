import json
import time
import unittest
import logging
from modules.unity_logging import AzureBlobHandler, LoggerConfiguration
from modules.authorisation import Authorisation
from modules.configuration import GlobalConfiguration
from unittest import TestCase
from unittest.mock import patch, call, Mock
import fixpath

class TestUnityLogging(TestCase):
    
    @classmethod
    def setUpClass(cls):

        #GlobalConfiguration().configure_tenant("inttestunityloggingtenant")

        cls.containers_list = ["logs", "inttestunityloggingtenant"]
        for container_name in cls.containers_list:
            client = Authorisation.get_container_client(container_name)
            if not client.exists():
                print(f"Creating container...{container_name}...")
                client.create_container()
                print(f"Container...{container_name}...created")

        LoggerConfiguration.ROOT_LOGGER_INITIALISED = False
        
        # Creating a custom logger for testing
        cls.shared_custom_logger = logging.getLogger('shared_test_logger')
        cls.tenant_custom_logger = logging.getLogger('tenant_test_logger')

    @classmethod
    def tearDownClass(cls):
        
        LoggerConfiguration.ROOT_LOGGER_INITIALISED = False

        cls.containers_list = ["inttestunityloggingtenant"]
        for container_name in cls.containers_list:
            client = Authorisation.get_container_client(container_name)
            if client.exists():
                print(f"Removing container...{container_name}...")
                client.delete_container()
                print(f"Container...{container_name}...removed")

    def tearDown(self):
        LoggerConfiguration.ROOT_LOGGER_INITIALISED = False

    ### Test configuration for shared logs
    def test_configure_shared_tenant(self):
        
        # Creating an instance of LoggerConfiguration
        logger_config = LoggerConfiguration(tenant_name='shared', level=logging.INFO)

        # Calling the configure method
        logger_config.configure(self.shared_custom_logger)
        
        # Assertions
        shared_log_file_name = next(((str(handler).split(" ")[1]).rstrip(">").lstrip("/") for handler in logging.getLogger('shared_test_logger').handlers if "AzureBlobHandler" in str(handler)), None)

        container_client = Authorisation().get_container_client("logs")
        blob_list = container_client.list_blob_names()
        time.sleep(20)
        self.assertIn(shared_log_file_name, list(blob_list))
        
    ### Test configuration for tenant logs
    def test_configure_test_tenant(self):


        # Creating an instance of LoggerConfiguration
        logger_config = LoggerConfiguration(tenant_name='inttestunityloggingtenant', level=logging.INFO)

        # Calling the configure method
        logger_config.configure(self.tenant_custom_logger)
        
        # Assertions
        tenant_log_file_name = next(((str(handler).split(" ")[1]).rstrip(">").lstrip("/") for handler in logging.getLogger('tenant_test_logger').handlers if "AzureBlobHandler" in str(handler)), None)

        container_client = Authorisation().get_container_client("inttestunityloggingtenant")
        blob_list = container_client.list_blob_names()
        time.sleep(20)
        self.assertIn(tenant_log_file_name, list(blob_list))