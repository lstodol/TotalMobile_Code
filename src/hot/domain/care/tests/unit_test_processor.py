import unittest
from unittest.mock import Mock
from hot.domain.care.handler.processor import Processor, new_processor

class TestDomainCareProcessor(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Setup method called once before all test methods"""

    def setUp(self):
        """Setup method called before every test method"""
        self.interactor = Mock()
        self.processor = new_processor(self.interactor)

    def test_handler_tenant_name_exception(self):
        with self.assertRaises(Exception) as context:
            self.processor.handler("", "domain_name", "table_name", "primary_keys", "data_layer")
        self.assertIn("Required argument \"tenant_name\" is empty", str(context.exception))
        
    def test_handler_domain_name_exception(self):
        with self.assertRaises(Exception) as context:
            self.processor.handler("tenant_name", "", "table_name", "primary_keys", "data_layer")
        self.assertIn("Required argument \"domain_name\" is empty", str(context.exception))

    def test_handler_invalid_table_data_layer_combination(self):
        with self.assertRaises(Exception) as context:
            self.processor.handler("tenant_name", "domain_name", "invalid_table", "primary_keys", "data_layer")
        self.assertIn("is not an accepted combination", str(context.exception))

    def test_handler_valid_call(self):
        self.processor.handler("tenant_name", "domain_name", "bulletin", "primary_keys", "silver")
        self.interactor.load_silver_care_bulletin_table.assert_called_once_with("tenant_name", "domain_name", "bulletin", "primary_keys", "silver")

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