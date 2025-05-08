import calendar
import time, json
import unittest

from databricks.sdk import WorkspaceClient
from datetime import datetime, timedelta
from modules.databricks_utils import DatabricksJobManager
from databricks.sdk.service import compute, jobs
from databricks.sdk.service.jobs import JobSettings
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from unittest import mock
from unittest.mock import call, patch, Mock
from modules.configuration_utils import ConfigurationUtils
from modules.exceptions import ClusterNotFoundException
from modules.configuration import GlobalConfiguration

# details for create task, job and run job
stage = "raw_to_bronze"
tenant = "inttest_du_djm_testtenant"
product = "inttest_du_djm_testproduct"
schema_name = "dbo"
table_name = "testtable"
table_type = "LIVE"
primary_key = "id"
incremental_column = 'ValidFrom'
dependent_task_1 = ""
dependent_task_2 = "deptask"
script_path = "dummypath"
SAMPLE_NOTEBOOK_NAME = "test_sample_notebook"

job_name_1 = "inttest_du_djm_jobname1"
job_name_2 = "inttest_du_djm_jobname2"
job_id = 1
tasks_list = []
job_config = {
    "JobTimeoutThreshold": 300,
    "JobDurationThreshold": 300,
    "NotificationRecipients": "abc@xyz.co.uk",
}

# used for testing DatabricksJobManager().get_cluster_id
TEST_CLUSTER_NAME_VALID = 'TEST_CLUSTER_VALID'
TEST_CLUSTER_NAME_NOT_VALID = 'TEST_CLUSTER_NOT_VALID'
TEST_CLUSTER_ID = '1129-110824-TESTID'
TEST_CLUSTER_DATA = [compute.ClusterDetails(cluster_id="1129-110824-TESTID", cluster_name="TEST_CLUSTER_VALID"),
                     compute.ClusterDetails(cluster_id="1130-090912-OTHER", cluster_name="TEST_CLUSTER_OTHER")]

# used for testing DatabricksJobManager().cleanup_jobs
TEST_RETENTION_PERIODS = {'sqa1': 7}
JOB_TIME_10 = str(datetime.utcfromtimestamp(round(time.time() - (86400 * 10)))) + ".000000"
JOB_TIME_2 = str(datetime.utcfromtimestamp(round(time.time() - (86400 * 2)))) + ".000000"

# simulate that jobs created 10 days and 2 days ago
TEST_JOB = [jobs.BaseJob(job_id=254520598349666, settings=JobSettings(name=f'bronze_sqa1_carelink_{JOB_TIME_10}')),
            jobs.BaseJob(job_id=254520598349888, settings=JobSettings(name=f'bronze_sqa1_carelink_{JOB_TIME_2}'))]

# simulate that tasks finished 2 minutes after jobs were created
TEST_RUN = [jobs.BaseRun(end_time=round(time.time() - (86400 * 10) + 120) * 1000, job_id=254520598349666, run_id=649017159084666),
            jobs.BaseRun(end_time=round(time.time() - (86400 * 2) + 120) * 1000, job_id=254520598349666, run_id=649017159084888)]


class TestDatabricksJobManager(unittest.TestCase):

    @classmethod
    def get_dbutils(cls):
        spark = SparkSession.getActiveSession()
        return DBUtils(spark)
    
    @classmethod
    def setUpClass(cls):
        print("Testing TestDatabricksJobManager - setUpClass...")
        
        cls.spark = SparkSession.builder.getOrCreate() 
        cls.dbutils = DBUtils(cls.spark)

        print("Testing TestDatabricksJobManager - setUpClass... DONE")

        global_config = ConfigurationUtils.read_config(
            GlobalConfiguration.GLOBAL_JOBS_CONFIG_PATH
        )
        
        cls.cluster_name = global_config["ClusterName"]
        print(f"Cluster name: {cls.cluster_name}")

        cls.dbutils = cls.get_dbutils()

        ########### PREPARE PATH SAMPLE NOTEBOOK ########################################################

        cls.context = json.loads(cls.dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())

        cls.current_path = cls.context['extraContext']['notebook_path'].rsplit('/', 1)[0]
        print(f"Current_path: {cls.current_path}")

        cls.sample_notebook_full_path = f"{cls.current_path}/{SAMPLE_NOTEBOOK_NAME}"
        print(f"Sample_notebook_full_path: {cls.sample_notebook_full_path}")


    def get_test_job(self, job_id):
        out_job = []
        for j in TEST_JOB:
            if j.job_id == job_id:
                out_job.append(j)
                break

        return out_job

    def get_test_runs_for_job(self, job_id):
        out_runs = []
        for r in TEST_RUN:
            if r.job_id == job_id:
                out_runs.append(r)

        return out_runs


    def test_create_task_without_dependancy(self):
        
        task = DatabricksJobManager().create_task(TEST_CLUSTER_NAME_VALID, stage, tenant, product, schema_name, table_name, table_type, primary_key, incremental_column, dependent_task_1, script_path)
        self.assertEqual(task.task_key, stage+"_"+schema_name+"_"+table_name, msg = "Task key / name is not matching")
        self.assertEqual(task.existing_cluster_id, TEST_CLUSTER_NAME_VALID, msg = "Cluster name is not matching")
        self.assertEqual(task.notebook_task.notebook_path, script_path, msg = "Script path is not matching")
        self.assertEqual(task.depends_on, None, msg = "There shouldn't be any dependancy")
        self.assertEqual(task.run_if, None, msg = "There shouldn't be any dependent condition for the task run")

    def test_create_task_with_dependancy(self):
        
        task = DatabricksJobManager().create_task(TEST_CLUSTER_NAME_VALID, stage, tenant, product, schema_name, table_name, table_type, primary_key, incremental_column, dependent_task_2, script_path)
        self.assertEqual(task.task_key, stage+"_"+schema_name+"_"+table_name, msg = "Task key / name is not matching")
        self.assertEqual(task.existing_cluster_id, TEST_CLUSTER_NAME_VALID, msg = "Cluster name is not matching")
        self.assertEqual(task.notebook_task.notebook_path, script_path, msg = "Script path is not matching")
        self.assertEqual((task.depends_on[0]).task_key, dependent_task_2, msg = "Dependent task should match")
        self.assertEqual(str(task.run_if).split(".")[1], "ALL_DONE", msg = "Dependent run condition should be ALL_DONE")
        
    # create job
    @patch.object(GlobalConfiguration, "get_job_config", return_value = job_config)
    def test_create_job(self, mocked_get_job_config):
        job = DatabricksJobManager().create_job(job_name_1, tasks_list, stage, tenant, product)
        job_id = job.job_id

        mocked_get_job_config.assert_called()

        if not isinstance(job_id, int):
            raise AssertionError("Job id should have been created")
        else:
            WorkspaceClient().jobs.delete(job_id)

    # run job
    @patch.object(GlobalConfiguration, "get_job_config", return_value = job_config)
    def test_run_job(self, mocked_get_job_config):

        print(f"Get cluster_id for cluster name {self.cluster_name}...")
        databricks_job_manager = DatabricksJobManager()
        self.cluster_id = databricks_job_manager.get_cluster_id(self.cluster_name)
        print(f"Cluster_id for cluster name {self.cluster_name} is: {self.cluster_id}")

        print(f"Creating sample task... Run notebook: {self.sample_notebook_full_path}")
        tasks_list = []

        task = DatabricksJobManager().create_task(self.cluster_id, stage, tenant, product, schema_name, table_name, table_type, primary_key, incremental_column, dependent_task_1, self.sample_notebook_full_path)
        tasks_list.append(task)
        
        job = DatabricksJobManager().create_job(job_name_2, tasks_list, stage, tenant, product)
        job_id = job.job_id

        run_id = DatabricksJobManager().run_job(job_id)

        print(job_id, run_id)

        if not isinstance(job_id, int):
            raise AssertionError("For a job run - Job id should have been created")
        elif not isinstance(run_id, int):
            WorkspaceClient().jobs.delete(job_id)
            raise AssertionError("Job should have been executed")
        else:
            WorkspaceClient().jobs.delete(job_id)


    @patch.object(jobs.JobsAPI, 'list', return_value=TEST_JOB)
    @patch.object(jobs.JobsAPI, 'delete')
    def test_cleanup_jobs_delete_job(self, mock_delete, mock_list):
        print("Test name: test_cleanup_jobs_delete_job")

        DatabricksJobManager().cleanup_jobs(thresholds = TEST_RETENTION_PERIODS)
        mock_delete.assert_called_once_with(254520598349666)

        print("Test name: test_cleanup_jobs_delete_job - DONE")

    @patch.object(jobs.JobsAPI, 'list', return_value=TEST_JOB)
    @patch.object(jobs.JobsAPI, 'delete')
    def test_cleanup_jobs_not_delete_job(self, mock_delete, mock_list):
        print("Test name: test_cleanup_jobs_not_delete_job")

        DatabricksJobManager().cleanup_jobs(thresholds = TEST_RETENTION_PERIODS)

        calls = [call(254520598349888)]
        with self.assertRaises(AssertionError, msg="Job deleted, but it is not expected.") as cm:
            mock_delete.assert_has_calls(calls)

        print("Test name: test_cleanup_jobs_not_delete_job - DONE")

    @classmethod
    def tearDownClass(cls):
        print("Testing TestDatabricksUtils - tearDownClass...")
        print("Testing TestDatabricksUtils - tearDownClass... DONE")