import calendar
import time
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
from modules.exceptions import ClusterNotFoundException
from modules.configuration import GlobalConfiguration

# details for create task, job and run job
stage = "raw_to_bronze"
tenant = "unittestdbxutilsdbxjobmanagertenant1"
product = "carelink"
schema_name = "dbo"
table_name = "testtable"
table_type = "LIVE"
primary_key = "id"
INCREMENTAL_COLUMN = 'ValidFrom'
dependent_task_1 = ""
dependent_task_2 = "deptask"
script_path = "dummypath"

job_name = "jobname"
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
    def setUpClass(cls):
        print("Testing TestDatabricksUtils - setUpClass...")
        
        #cls.spark = SparkSession.builder.getOrCreate() 
        #cls.dbutils = DBUtils(cls.spark)

        print("Testing TestDatabricksUtils - setUpClass... DONE")
    
    ### Test get_cluster_id for a valid cluster 
    def test_get_cluster_id_valid(self):
        print("Test name: test_get_cluster_id_valid")
        
        with patch.object(compute.ClustersAPI, 'list', return_value=TEST_CLUSTER_DATA) as mocked_clusters_list:
            cluster_id = DatabricksJobManager().get_cluster_id(TEST_CLUSTER_NAME_VALID)

            self.assertEqual(cluster_id, TEST_CLUSTER_ID)
        
        print("Test name: test_get_cluster_id_valid - DONE")

    ### Test get_cluster_id for an invalid cluster
    @patch.object(compute.ClustersAPI, 'list', return_value=TEST_CLUSTER_DATA)
    def test_get_cluster_id_is_not_valid(self, mock1):
        print("Test name: test_get_cluster_id_is_not_valid")
        
        with self.assertRaises(ClusterNotFoundException, msg="Different exception than ClusterNotFoundException is not expected.") as cm:
            DatabricksJobManager().get_cluster_id(TEST_CLUSTER_NAME_NOT_VALID)

        print("Test name: test_get_cluster_id_is_not_valid - DONE")

    ### Test create_task for passed arguments
    def test_create_task_args(self):
        print("Test name: test_create_task_args")
        #missing script path argument
        with self.assertRaises(TypeError, msg = "Error should be raised for Missing argument") as cm:
            DatabricksJobManager().create_task(TEST_CLUSTER_NAME_VALID, stage, tenant, product, schema_name, table_name, table_type, primary_key, INCREMENTAL_COLUMN, dependent_task_1)
        
        #extra argument
        testparam = ""
        with self.assertRaises(TypeError, msg = "Error should be raised for extra argument") as cm:
            DatabricksJobManager().create_task(TEST_CLUSTER_NAME_VALID, stage, tenant, product, schema_name, table_name, table_type, primary_key, INCREMENTAL_COLUMN, dependent_task_1, script_path, testparam)
        
        print("Test name: test_create_task_args - DONE")

    ### Test create_job for passed arguments
    def test_create_job_args(self):
        print("Test name: test_create_job_args")
        #missing product argument
        with self.assertRaises(TypeError, msg = "Error should be raised for Missing argument") as cm:
            DatabricksJobManager().create_job(job_name, tasks_list, stage, tenant)
        
        #extra argument
        testparam = ""
        with self.assertRaises(TypeError, msg = "Error should be raised for extra argument") as cm:
            DatabricksJobManager().create_job(job_name, tasks_list, stage, tenant, product, testparam)
    
        print("Test name: test_create_job_args - DONE")

    ### Test create_job 
    @patch.object(GlobalConfiguration, "get_job_config", return_value = job_config)
    def test_create_job(self, mocked_get_job_config):
        print("Test name: test_create_job")

        with patch.object(
            jobs.JobsAPI, "create"
            ) as mocked_create:
            DatabricksJobManager().create_job(job_name, tasks_list, stage, tenant, product)
        
        mocked_get_job_config.assert_called()
        mocked_create.assert_called()
        print("Test name: test_create_job - DONE")

    ### Tets run_job for passed args
    def test_run_job_args(self):
        print("Test name: test_run_job_args")
        #missing product argument
        with self.assertRaises(TypeError, msg = "Error should be raised for Missing argument") as cm:
            DatabricksJobManager().run_job()
        
        #extra argument
        testparam = ""
        with self.assertRaises(TypeError, msg = "Error should be raised for extra argument") as cm:
            DatabricksJobManager().run_job(job_id, testparam)
        
        print("Test name: test_run_job_args - DONE")
    
    ### Tets run_job
    def test_run_job(self):
        print("Test name: test_run_job")
        with patch.object(
            jobs.JobsAPI, "run_now"
            ) as mocked_run:
            DatabricksJobManager().run_job(job_id)
        
        mocked_run.assert_called_once_with(job_id=job_id)
        print("Test name: test_run_job - DONE")

    ### Test convert_unix_to_datetime
    def test_convert_unix_to_datetime(self):
        print("Test name: test_convert_unix_to_datetime")
        # convert 1701097270000 --> 2023-11-27 15:01:10
        converted_datetime = DatabricksJobManager.convert_unix_to_datetime(1701097270000)

        expected_datetime = datetime(2023, 11, 27, 15, 1, 10)

        self.assertEqual(expected_datetime, converted_datetime, 
                         "Wrong DateTime returned: " + str(converted_datetime) + ". Expected DateTime is: " + str(expected_datetime))
        
        print("Test name: test_convert_unix_to_datetime - DONE")

    @classmethod
    def tearDownClass(cls):
        print("Testing TestDatabricksUtils - tearDownClass...")
        print("Testing TestDatabricksUtils - tearDownClass... DONE")