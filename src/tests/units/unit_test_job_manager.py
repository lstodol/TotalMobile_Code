from unittest import TestCase
from unittest.mock import patch, call
import fixpath
from pyspark.sql import SparkSession
from modules.job_manager import JobManager
from modules.databricks_utils import AuditLogger, DatabricksJobManager
from modules.configuration import GlobalConfiguration
from databricks.sdk.service import jobs
from modules.exceptions import ExceptionCatcher, UnityExecutionException

spark = SparkSession.builder.getOrCreate()

class TestJobManager(TestCase):
    
    @classmethod
    def setUpClass(cls):

        cls.spark = SparkSession.builder.getOrCreate()

        cls.tenant_dict = {"unittestjobmanagertenant1": ["carelink", "mobilise", "datamart"],
                           "unittestjobmanagertenant2": ["carelink"]
                           }
        cls.product_dict = {"carelink": ["dbo|table1|LIVE|id|ValidFrom", "dbo|table2|LIVE|id|ValidFrom", "dbo|table3|LIVE|id|ValidFrom"]}
        cls.layer = "bronze"

        cls.tenant = "unittestjobmanagertenant3"
        cls.product = "carelink"

        cls.job_output = jobs.BaseJob(
            job_id=1,
            settings=jobs.JobSettings(
                name="jobname",
                tasks=[
                    jobs.Task(task_key="10"),
                    jobs.Task(task_key="11"),
                    jobs.Task(task_key="12"),
                ],
            ),
        )

        cls.context = {'is_this_a_test_context': True}
        cls.exception_catcher = ExceptionCatcher(cls.context)

    ### Test create_and_run_job method for an already running job
    @patch.object(AuditLogger, "read_job_audit_table", return_value = "RUNNING")
    def test_create_and_run_job_running(self, mocked_read_job_audit_table):
        print("Test name: test_create_and_run_job_running")
        # arrange
        with self.assertLogs(None, level='WARN') as cm:
            job_manager = JobManager(self.tenant_dict, self.layer)
            job_manager.create_and_run_job()

        # assert
        mocked_read_job_audit_table.assert_has_calls([call(), call(), call()])
        self.assertIn("WARNING:modules.job_manager:Task and job creation halted because previous job run is in RUNNING state.", cm.output)

        print("Test name: test_create_and_run_job_running - DONE")

    ### Test create_and_run_job method for a previously failed job
    @patch.object(AuditLogger, "read_job_audit_table", return_value = "FAILED")
    def test_create_and_run_job_failed(self, mocked_read_job_audit_table):
        # arrange
        job_manager = JobManager(self.tenant_dict, self.layer)
        job_manager.create_and_run_job()

        # assert
        mocked_read_job_audit_table.assert_has_calls([call(), call(), call()])

    ### Test create_and_run_job method for previously success job
    @patch.object(AuditLogger, "read_job_audit_table", return_value = "SUCCESS")
    @patch.object(JobManager, "parallelisation", return_value = {1,2,3,4,5})
    @patch.object(JobManager, "get_tasks_full_list", return_value = [])
    @patch.object(AuditLogger, "job_create_update")
    @patch.object(DatabricksJobManager, "run_job", return_value = 123)
    @patch.object(AuditLogger, "job_run_update")
    def test_create_and_run_job_success(self, mocked_job_run_update, mocked_run_job, mocked_job_create_update, mocked_get_tasks_full_list, mocked_parallelisation, mocked_read_job_audit_table):

        # arrange
        with patch.object(DatabricksJobManager, "create_job", return_value = self.job_output) as mocked_create_job:
            job_manager = JobManager(self.tenant_dict, self.layer, None, self.product_dict)
            job_manager.create_and_run_job()

        # assert
        mocked_read_job_audit_table.assert_has_calls([call(), call(), call(), call()])
        mocked_parallelisation.assert_has_calls([call('unittestjobmanagertenant1', 'carelink'),
                                                 call('unittestjobmanagertenant1', 'mobilise'),
                                                 call('unittestjobmanagertenant1', 'datamart'),  
                                                 call('unittestjobmanagertenant2', 'carelink')])
        mocked_get_tasks_full_list.assert_has_calls([call('unittestjobmanagertenant1', 'carelink', 1, 2, 3, 4, 5),
                                                     call('unittestjobmanagertenant1', 'mobilise', 1, 2, 3, 4, 5),
                                                     call('unittestjobmanagertenant1', 'datamart', 1, 2, 3, 4, 5),
                                                     call('unittestjobmanagertenant2', 'carelink', 1, 2, 3, 4, 5)])
        mocked_create_job.assert_called()
        mocked_job_create_update.assert_has_calls([call(1), 
                                                  call(1), 
                                                  call(1),
                                                  call(1)])
        mocked_run_job.assert_has_calls([call(1),
                                         call(1), 
                                         call(1),
                                         call(1)])
        mocked_job_run_update.assert_has_calls([call(123),
                                                call(123), 
                                                call(123),
                                                call(123)])

    ### Test create_and_run_job method for a first time run
    @patch.object(AuditLogger, "read_job_audit_table", return_value = "FIRST_RUN")
    @patch.object(JobManager, "parallelisation", return_value = {5, 4, 3, 2, 1})
    @patch.object(JobManager, "get_tasks_full_list", return_value = [])
    @patch.object(AuditLogger, "job_create_update")
    @patch.object(DatabricksJobManager, "run_job", return_value = 123)
    @patch.object(AuditLogger, "job_run_update")
    def test_create_and_run_job_first_run(self, mocked_job_run_update, mocked_run_job, mocked_job_create_update, mocked_get_tasks_full_list, mocked_parallelisation, mocked_read_job_audit_table):
        # arrange
        with patch.object(DatabricksJobManager, "create_job", return_value = self.job_output) as mocked_create_job:
            job_manager = JobManager(self.tenant_dict, self.layer, None, self.product_dict)
            job_manager.create_and_run_job()

        # assert
        mocked_read_job_audit_table.assert_has_calls([call(), call(), call(), call()])
        mocked_parallelisation.assert_has_calls([call('unittestjobmanagertenant1', 'carelink'),
                                                 call('unittestjobmanagertenant1', 'mobilise'), 
                                                 call('unittestjobmanagertenant1', 'datamart'),
                                                 call('unittestjobmanagertenant2', 'carelink')])
        mocked_get_tasks_full_list.assert_has_calls([call('unittestjobmanagertenant1', 'carelink', 1, 2, 3, 4, 5),
                                                     call('unittestjobmanagertenant1', 'mobilise', 1, 2, 3, 4, 5),
                                                     call('unittestjobmanagertenant1', 'datamart', 1, 2, 3, 4, 5),
                                                     call('unittestjobmanagertenant2', 'carelink', 1, 2, 3, 4, 5)])
        mocked_create_job.assert_called()
        mocked_job_create_update.assert_has_calls([call(1), 
                                                  call(1),  
                                                  call(1),
                                                  call(1)])
        mocked_run_job.assert_has_calls([call(1),
                                         call(1),
                                         call(1), 
                                         call(1)])
        mocked_job_run_update.assert_has_calls([call(123),
                                                call(123),
                                                call(123), 
                                                call(123)])

    ### Test parallelisation method where parallel streams parameter is set to be a negative number
    @patch.object(GlobalConfiguration, "get_job_config", return_value = {"TasksParallelStreams": -2})
    def test_parallelisation_negative_parallel_stream(self, mocked_get_job_config):
        # arrange
        job_manager = JobManager(self.tenant_dict, self.layer, None, self.product_dict)
        total_tasks, parallel_streams, nearest_multiple_parallel_tasks, max_tasks_streams, remaining_tasks_streams = job_manager.parallelisation(self.tenant, self.product)
        
        self.assertEqual(total_tasks, 3, msg="Total tasks value not matching")
        self.assertEqual(parallel_streams, 1, msg="parallel_streams value not matching")
        self.assertEqual(nearest_multiple_parallel_tasks, 3, msg="nearest_multiple_parallel_tasks value not matching")
        self.assertEqual(max_tasks_streams, 0, msg="max_tasks_streams value not matching")
        self.assertEqual(remaining_tasks_streams, 1, msg="remaining_tasks_streams value not matching")

    ### Test parallelisation method where parallel streams parameter is set to be 0
    @patch.object(GlobalConfiguration, "get_job_config", return_value = {"TasksParallelStreams": 0})
    def test_parallelisation_zero_parallel_stream(self, mocked_get_job_config):
        # arrange
        job_manager = JobManager(self.tenant_dict, self.layer, None, self.product_dict)
        total_tasks, parallel_streams, nearest_multiple_parallel_tasks, max_tasks_streams, remaining_tasks_streams = job_manager.parallelisation(self.tenant, self.product)
        
        self.assertEqual(total_tasks, 3, msg="Total tasks value not matching")
        self.assertEqual(parallel_streams, 1, msg="parallel_streams value not matching")
        self.assertEqual(nearest_multiple_parallel_tasks, 3, msg="nearest_multiple_parallel_tasks value not matching")
        self.assertEqual(max_tasks_streams, 0, msg="max_tasks_streams value not matching")
        self.assertEqual(remaining_tasks_streams, 1, msg="remaining_tasks_streams value not matching")

    ### Test parallelisation method where parallel streams parameter is set to be 2
    @patch.object(GlobalConfiguration, "get_job_config", return_value = {"TasksParallelStreams": 2})
    def test_parallelisation_two_parallel_stream(self, mocked_get_job_config):
        # arrange
        job_manager = JobManager(self.tenant_dict, self.layer, None, self.product_dict)
        total_tasks, parallel_streams, nearest_multiple_parallel_tasks, max_tasks_streams, remaining_tasks_streams = job_manager.parallelisation(self.tenant, self.product)
        
        self.assertEqual(total_tasks, 3, msg="Total tasks value not matching")
        self.assertEqual(parallel_streams, 2, msg="parallel_streams value not matching")
        self.assertEqual(nearest_multiple_parallel_tasks, 2, msg="nearest_multiple_parallel_tasks value not matching")
        self.assertEqual(max_tasks_streams, 1, msg="max_tasks_streams value not matching")
        self.assertEqual(remaining_tasks_streams, 1, msg="remaining_tasks_streams value not matching")

    ### Test parallelisation method where parallel streams parameter is set to be 20
    @patch.object(GlobalConfiguration, "get_job_config", return_value = {"TasksParallelStreams": 20})
    def test_parallelisation_twenty_parallel_stream(self, mocked_get_job_config):
        # arrange
        job_manager = JobManager(self.tenant_dict, self.layer, None, self.product_dict)
        total_tasks, parallel_streams, nearest_multiple_parallel_tasks, max_tasks_streams, remaining_tasks_streams = job_manager.parallelisation(self.tenant, self.product)
        
        self.assertEqual(total_tasks, 3, msg="Total tasks value not matching")
        self.assertEqual(parallel_streams, 20, msg="parallel_streams value not matching")
        self.assertEqual(nearest_multiple_parallel_tasks, 0, msg="nearest_multiple_parallel_tasks value not matching")
        self.assertEqual(max_tasks_streams, 3, msg="max_tasks_streams value not matching")
        self.assertEqual(remaining_tasks_streams, 17, msg="remaining_tasks_streams value not matching")

    ### Test get_tasks_full_list method for tasks in series
    @patch.object(JobManager, "get_tasks_subset_list", return_value = ([jobs.Task(task_key="test")], 10))
    @patch.object(JobManager, "task_creation")
    def test_get_tasks_full_list_series(self, mocked_task_creation, mocked_get_tasks_subset_list):
        # arrange
        job_manager = JobManager(self.tenant_dict, self.layer, None, self.product_dict)
        job_manager.get_tasks_full_list(self.tenant, self.product, 5, 4, 3, 2, 1)

        mocked_get_tasks_subset_list.assert_called()
        mocked_get_tasks_subset_list.assert_has_calls([call(self.tenant, self.product, 2, 1, 0, "series"),
                                                       call(self.tenant, self.product, 1, 0, 10, "series")])
        mocked_task_creation.assert_not_called()

    ### Test get_tasks_full_list method for tasks in parallel
    @patch.object(JobManager, "get_tasks_subset_list")
    @patch.object(JobManager, "task_creation")
    def test_get_tasks_full_list_parallel(self, mocked_task_creation, mocked_get_tasks_subset_list):
        # arrange
        job_manager = JobManager(self.tenant_dict, self.layer, None, self.product_dict)
        job_manager.get_tasks_full_list(self.tenant, self.product, 5, 4, 3, 5, 1)

        mocked_get_tasks_subset_list.assert_not_called()
        mocked_task_creation.assert_has_calls([call(self.tenant, self.product, self.product_dict[self.product], "parallel")])

    ### Test get_tasks_subset_list method 
    @patch.object(JobManager, "task_creation")
    def test_get_tasks_subset_list(self, mocked_task_creation):
        # arrange
        job_manager = JobManager(self.tenant_dict, self.layer, None, self.product_dict)
        job_manager.get_tasks_subset_list(self.tenant, self.product, 2, 1, 0, "series")

        mocked_task_creation.assert_called()

    ### Test task_creation method
    @patch.object(DatabricksJobManager, "create_task", return_value = jobs.Task(task_key = "test"))
    def test_task_creation(self, mocked_create_task):
        # arrange
        job_manager = JobManager(self.tenant_dict, self.layer, None, self.product_dict)
        job_manager.task_creation(self.tenant, self.product, self.product_dict[self.product], "series")

        mocked_create_task.assert_called()
        mocked_create_task.assert_has_calls([call(None, self.layer, self.tenant, self.product, 'dbo', 'table1', 'LIVE', 'id', 'ValidFrom', '', None),
                                             call(None, self.layer, self.tenant, self.product, 'dbo', 'table2', 'LIVE', 'id', 'ValidFrom', 'test', None),
                                             call(None, self.layer, self.tenant, self.product, 'dbo', 'table3', 'LIVE', 'id', 'ValidFrom', 'test', None)])

    ### Test task_creation method for warnings
    @patch.object(DatabricksJobManager, "create_task", return_value = "")
    def test_task_creation_warning(self, mocked_create_task):
        # arrange

        with self.assertLogs(None, level='WARN') as cm:
            job_manager = JobManager(self.tenant_dict, self.layer, None, self.product_dict)
            job_manager.task_creation(self.tenant, self.product, self.product_dict[self.product], "parallel")

        self.assertIn("WARNING:modules.job_manager:Issue in task creation for table: table1.", cm.output)
        self.assertIn("WARNING:modules.job_manager:Issue in task creation for table: table2.", cm.output)
        self.assertIn("WARNING:modules.job_manager:Issue in task creation for table: table3.", cm.output)

    ### Test monitor_job method when no job is running
    @patch.object(AuditLogger, "get_active_jobs", return_value = spark.createDataFrame([], "string").toDF("job_run_id"))
    @patch.object(AuditLogger, "job_run_update")
    def test_monitor_job_no_run(self, mocked_job_run_update, mocked_get_active_jobs):
        # arrange
        job_manager = JobManager(self.tenant_dict, self.layer)
        job_manager.monitor_job()

        mocked_get_active_jobs.assert_has_calls([call(), call(), call()])
        mocked_job_run_update.assert_not_called()

    ### Test monitor_job method when one job is running
    @patch.object(AuditLogger, "get_active_jobs", return_value = spark.createDataFrame(["123"], "string").toDF("job_run_id"))
    @patch.object(AuditLogger, "job_run_update")
    def test_monitor_job_one_run(self, mocked_job_run_update, mocked_get_active_jobs):
        # arrange
        job_manager = JobManager(self.tenant_dict, self.layer)
        job_manager.monitor_job()

        mocked_get_active_jobs.assert_has_calls([call(), call(), call()])
        mocked_job_run_update.assert_called()
        mocked_job_run_update.assert_has_calls([call("123"), call("123"), call("123")])

    ### Test monitor_job method when two jobs are running
    @patch.object(AuditLogger, "get_active_jobs", return_value = spark.createDataFrame(["123", "456"], "string").toDF("job_run_id"))
    @patch.object(AuditLogger, "job_run_update")
    def test_monitor_job_two_runs(self, mocked_job_run_update, mocked_get_active_jobs):
        # arrange
        job_manager = JobManager(self.tenant_dict, self.layer)
        job_manager.monitor_job()

        mocked_get_active_jobs.assert_has_calls([call(), call(), call()])
        mocked_job_run_update.assert_called()
        mocked_job_run_update.assert_has_calls([call("123"), call("456"), call("123"), call("456"), call("123"), call("456")])