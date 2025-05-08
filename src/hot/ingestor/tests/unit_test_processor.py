import unittest
import fixpath
from unittest.mock import Mock
from hot.ingestor.handler.processor import Processor, new_processor


class TestIngestorProcessor(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Setup method called once before all test methods"""

    def setUp(self):
        """Setup method called before every test method"""
        self.interactor = Mock()
        self.processor = new_processor(self.interactor)

    def test_handler_tenant_name_type_error_exception(self):
        with self.assertRaises(TypeError , msg=f"Required argument \"tenant_name\" is not empty."):
            self.processor.handler(None, "product_name", "entity_name")
        
    def test_handler_product_name_type_error_exception(self):
        with self.assertRaises(TypeError , msg=f"Required argument \"product_name\" is not empty."):
            self.processor.handler("tenant_name", None, "entity_name")

    def test_handler_entity_name_type_error_exception(self):
        with self.assertRaises(TypeError , msg=f"Required argument \"entity_name\" is not empty."):
            self.processor.handler("tenant_name", "product_name", None)


    def test_handler_call_interactor(self):
        self.processor.handler("tenant_name", "product_name", "entity_name")

        self.interactor.load_bronze_eventhub_entity.assert_called_once_with("tenant_name", "product_name", "entity_name")

    def tearDown(self):
        """Teardown method called after every test method"""
        del self.interactor
        del self.processor

    @classmethod
    def tearDownClass(cls):
        """Teardown method called once after all test methods"""

if __name__ == '__main__':
    test_runner = unittest.main(argv=[''], exit=False)
    test_runner.result.printErrors()
