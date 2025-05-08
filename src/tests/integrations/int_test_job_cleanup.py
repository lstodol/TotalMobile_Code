import json
import os
import time
import unittest

from databricks.sdk.service import jobs
from databricks.sdk.errors.platform import InvalidParameterValue
from datetime import datetime, timedelta
from modules.authorisation import Authorisation
from modules.databricks_utils import DatabricksJobManager
from modules.configuration import GlobalConfiguration
from modules.configuration_utils import ConfigurationUtils
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

DEPENDENT_TASK = ''
PRIMARY_KEY = 'Id'
INCREMENTAL_COLUMN = 'ValidFrom'

PRODUCT_NAME = "TestProduct"
SAMPLE_NOTEBOOK_NAME = "test_sample_notebook"
SCHEMA_NAME = 'dbo'
STAGE_NAME = 'bronze'
TABLE_NAME = 'IntegrationTestJobCleanupTable'
TABLE_TYPE = 'REFERENCE'
TENANT_NAME = "IntegrationTestJobCleanup"
TENANT_NAME_NOT_FOR_DELETE = "IntegrationTestNotForDeleteJobCleanup"

class TestJobCleanup(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        print("Testing Job Cleanup script... setUpClass...")
        
        global_config = ConfigurationUtils.read_config(
            GlobalConfiguration.GLOBAL_JOBS_CONFIG_PATH
        )
        
        self.cluster_name = global_config["ClusterName"]
        print(f"Cluster name: {self.cluster_name}.")

        self.dbutils =  DBUtils(SparkSession.getActiveSession())

        ########### PREPARE PATH SAMPLE NOTEBOOK ########################################################

        self.context = json.loads(self.dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())

        self.current_path = self.context['extraContext']['notebook_path'].rsplit('/', 1)[0]
        print(f"Current_path: {self.current_path}.")

        self.sample_notebook_full_path = f"{self.current_path}/{SAMPLE_NOTEBOOK_NAME}"
        print(f"Sample_notebook_full_path: {self.sample_notebook_full_path}.")

        print("Testing Job Cleanup script... setUpClass... DONE")

    def test_cleanup_job_remove_job(self):
        print("Test name: test_cleanup_job_remove_job...")

        ########### CREATE TEST JOB WITH ONE TASK #################################

        print(f"Get cluster_id for cluster name {self.cluster_name}...")
        databricks_job_manager = DatabricksJobManager()
        self.cluster_id = databricks_job_manager.get_cluster_id(self.cluster_name)
        print(f"Cluster_id for cluster name {self.cluster_name} is: {self.cluster_id}.")

        print(f"Create sample task... Run notebook: {self.sample_notebook_full_path}.")
        tasks_list = []

        task = databricks_job_manager.create_task(self.cluster_id, STAGE_NAME, TENANT_NAME, PRODUCT_NAME, SCHEMA_NAME, 
                                                  TABLE_NAME, TABLE_TYPE, PRIMARY_KEY, INCREMENTAL_COLUMN, DEPENDENT_TASK, self.sample_notebook_full_path)

        tasks_list.append(task)
        tasks_list_count = len(tasks_list)
        print(f"Number of tasks created: {tasks_list_count}.")

        print(f"Create sample job...")
        dts = datetime.now() - timedelta(days=2)
        self.job_name = f"{STAGE_NAME}_{TENANT_NAME}_{PRODUCT_NAME}_{dts}"
        job = DatabricksJobManager().create_job(self.job_name, tasks_list, STAGE_NAME, TENANT_NAME, PRODUCT_NAME)
        self.job_id = job.job_id
        print(f"Create sample job {self.job_id}... DONE")

        ########### RUN cleanup_jobs (should remove job) ##########################

        print(f"Running DatabricksJobManager.cleanup_jobs...")
        DatabricksJobManager().cleanup_jobs({'IntegrationTestJobCleanup': 1})
        print(f"Running DatabricksJobManager.cleanup_jobs... DONE")

        ########### ASSERT - CHECK IF JOB WAS REMOVED #############################

        with self.assertRaises(InvalidParameterValue, 
                               msg=f"Different exception than InvalidParameterValue is not excepted. Job {self.job_name} was not deleted, but it should be."):
            self.w = DatabricksJobManager.databricks_login()
            self.w.jobs.get(self.job_id)

        print("Test name: test_cleanup_job_remove_job... DONE")

    def test_cleanup_job_not_remove_job_by_retention_period(self):
        print("Test name: test_cleanup_job_not_remove_job_by_retention_period...")

        ########### CREATE TEST JOB WITH ONE TASK #################################
        print(f"Get cluster_id for cluster name {self.cluster_name}...")
        databricks_job_manager = DatabricksJobManager()
        self.cluster_id = databricks_job_manager.get_cluster_id(self.cluster_name)
        print(f"Cluster_id for cluster name {self.cluster_name} is: {self.cluster_id}.")

        print(f"Create sample task... Run notebook: {self.sample_notebook_full_path}.")
        tasks_list = []

        task = databricks_job_manager.create_task(self.cluster_id, STAGE_NAME, TENANT_NAME, PRODUCT_NAME, SCHEMA_NAME, 
                                                  TABLE_NAME, TABLE_TYPE, PRIMARY_KEY, INCREMENTAL_COLUMN, DEPENDENT_TASK, self.sample_notebook_full_path)

        tasks_list.append(task)
        tasks_list_count = len(tasks_list)
        print(f"Number of tasks created: {tasks_list_count}.")

        print(f"Create sample job...")
        dts = datetime.now() - timedelta(days=1)
        self.job_name = f"{STAGE_NAME}_{TENANT_NAME}_{PRODUCT_NAME}_{dts}"
        job = DatabricksJobManager().create_job(self.job_name, tasks_list, STAGE_NAME, TENANT_NAME, PRODUCT_NAME)
        self.job_id = job.job_id
        print(f"Create sample job {self.job_id}... DONE")

        ########### RUN cleanup_jobs (should NOT remove job) ######################
        
        print(f"Running DatabricksJobManager.cleanup_jobs...")
        DatabricksJobManager().cleanup_jobs({'IntegrationTestJobCleanup': 1})
        print(f"Running DatabricksJobManager.cleanup_jobs... DONE")

        ########### ASSERT - CHECK IF JOB WAS NOT REMOVED #########################

        self.w = DatabricksJobManager.databricks_login()
        job_details_from_workspace = self.w.jobs.get(self.job_id)
        
        self.assertEqual(self.job_name, job_details_from_workspace.settings.name, msg=f"Job {self.job_name} not found, but expected that job exists.")

        ########### DROP JOB ######################################################
        print(f"Dropping job {self.job_name} with job_id {self.job_id}...")
        self.w = DatabricksJobManager.databricks_login()
        self.w.jobs.delete(self.job_id)

        print("Test name: test_cleanup_job_not_remove_job_by_retention_period... DONE")

    def test_cleanup_job_not_remove_job_by_tenant_name(self):
        print("Test name: test_cleanup_job_not_remove_job_by_tenant_name...")

        ########### CREATE TEST JOB WITH ONE TASK #################################
        print(f"Get cluster_id for cluster name {self.cluster_name}...")
        databricks_job_manager = DatabricksJobManager()
        self.cluster_id = databricks_job_manager.get_cluster_id(self.cluster_name)
        print(f"Cluster_id for cluster name {self.cluster_name} is: {self.cluster_id}.")

        print(f"Create sample task... Run notebook: {self.sample_notebook_full_path}.")
        tasks_list = []

        task = databricks_job_manager.create_task(self.cluster_id, STAGE_NAME, TENANT_NAME_NOT_FOR_DELETE, PRODUCT_NAME, SCHEMA_NAME, 
                                                  TABLE_NAME, TABLE_TYPE, PRIMARY_KEY, INCREMENTAL_COLUMN, DEPENDENT_TASK, self.sample_notebook_full_path)

        tasks_list.append(task)
        tasks_list_count = len(tasks_list)
        print(f"Number of tasks created: {tasks_list_count}.")

        print(f"Create sample job...")
        dts = datetime.now() - timedelta(days=2)
        self.job_name = f"{STAGE_NAME}_{TENANT_NAME_NOT_FOR_DELETE}_{PRODUCT_NAME}_{dts}"
        job = DatabricksJobManager().create_job(self.job_name, tasks_list, STAGE_NAME, TENANT_NAME_NOT_FOR_DELETE, PRODUCT_NAME)
        self.job_id = job.job_id
        print(f"Create sample job {self.job_id}... DONE")

        ########### RUN cleanup_jobs (should NOT remove job) ######################
       
        print(f"Running DatabricksJobManager.cleanup_jobs...")
        DatabricksJobManager().cleanup_jobs({'IntegrationTestJobCleanup': 1})
        print(f"Running DatabricksJobManager.cleanup_jobs... DONE")

        ########### ASSERT - CHECK IF JOB WAS NOT REMOVED #########################

        self.w = DatabricksJobManager.databricks_login()
        job_details_from_workspace = self.w.jobs.get(self.job_id)
        
        self.assertEqual(self.job_name, job_details_from_workspace.settings.name, msg=f"Job {self.job_name} not found, but expected that job exists.")

        ########### DROP JOB ######################################################
        print(f"Dropping job {self.job_name} with job_id {self.job_id}...")
        self.w = DatabricksJobManager.databricks_login()
        self.w.jobs.delete(self.job_id)

        print("Test name: test_cleanup_job_not_remove_job_by_tenant_name... DONE")

    @classmethod
    def tearDownClass(cls):
        print("Testing Job Cleanup script... tearDownClass...")

        print("Testing Job Cleanup script... tearDownClass... DONE")
    