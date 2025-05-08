import unittest
from unittest.mock import patch
from modules.exceptions import ExceptionCatcher, UnityExecutionException

class TestExceptionCatcher(unittest.TestCase):
    def setUp(self):
        
        self.context = {'is_this_a_test_context': True}
        self.exception_catcher = ExceptionCatcher(self.context)

    ### Test catch exceptions method for multiple excpetions
    def test_catch_and_throw_if_any(self):
        with self.assertRaises(UnityExecutionException) as context:

            self.exception_catcher.catch(ValueError("The exception no 1 has been caught!"))
            self.exception_catcher.catch(RuntimeError("Ohh I forgot to test the execution with 2 exceptions!"))
            self.exception_catcher.throw_if_any()

        self.assertIn("2 exception(s) were caught during job execution.", str(context.exception))
        self.assertIn("Context: {'is_this_a_test_context': True}, Exception: The exception no 1 has been caught!", str(context.exception))
        self.assertIn("Context: {'is_this_a_test_context': True}, Exception: Ohh I forgot to test the execution with 2 exceptions!", str(context.exception))

    ### Test set_context method
    def test_set_context(self):
        self.exception_catcher.set_context(job_id=123, step="Processing")
        self.assertEqual(self.exception_catcher.context, {'job_id': 123, 'step': 'Processing','is_this_a_test_context': True})

    ### Test catch exception method
    def test_catch_logs_error(self):

        exception = ValueError("Test exception")
        self.exception_catcher.catch(exception)

        expected_exception = {"exception": exception, "context":{'is_this_a_test_context': True}}
        self.assertIn(expected_exception, self.exception_catcher.exceptions_list)

          
