import json
import os
import time
import unittest

from databricks.sdk.service import jobs
from datetime import datetime
from modules.authorisation import Authorisation
from modules.databricks_utils import AuditLogger, DatabricksJobManager
from modules.configuration import GlobalConfiguration
from modules.configuration_utils import ConfigurationUtils
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from modules.exceptions import UnityExecutionException

DEPENDENT_TASK = ''
INT_MOUNT_POINT = '/mnt/IntegrationTestJobMonitoring'
INT_MOUNT_POINT_SEC = '/mnt/IntegrationTestJobMonitoringSecond'
JOB_MONITORIG_NOTEBOOK_NAME = 'job_monitoring'
PRIMARY_KEY = 'Id'
INCREMENTAL_COLUMN = ''
PRODUCT_NAME = "IntegrationTestJobMonitoringProduct"
SAMPLE_NOTEBOOK_NAME = "test_sample_notebook"
SAMPLE_NOTEBOOK_NAME_NOT_EXISTS = "test_sample_notebook_not_exists"
SCHEMA_NAME = 'dbo'
STAGE_NAME = 'bronze'
TABLE_NAME = 'IntegrationTestJobMonitoringTable'
TABLE_TYPE = 'REFERENCE'
TENANT_NAME = "IntegrationTestJobMonitoring"
TENANT_NAME_SEC = "IntegrationTestJobMonitoringSecond"
TENANT_PRODUCT_CONFIG = {'IntegrationTestJobMonitoring': ['IntegrationTestJobMonitoringProduct'], 
                         'IntegrationTestJobMonitoringSecond': ['IntegrationTestJobMonitoringProduct'],
                         'IntegrationTestJobMonitoringFailing': ['IntegrationTestJobMonitoringProduct']}

ENV_TENANT_CONFIG_FOLDER = f"{INT_MOUNT_POINT}/config/env_config"
ENV_TENANT_CONFIG_PATH = f"{ENV_TENANT_CONFIG_FOLDER}/tenant_config.json"  
ENV_TENANT_CONFIG_DBFS_PATH = f"/dbfs{ENV_TENANT_CONFIG_FOLDER}/tenant_config.json"  

JOB_AUDIT_TABLE = "job_audit"
JOB_AUDIT_TABLE_NAME = f"{TENANT_NAME}.{STAGE_NAME}_{PRODUCT_NAME}_{JOB_AUDIT_TABLE}"
JOB_AUDIT_TABLE_LOC = f"{INT_MOUNT_POINT}/{STAGE_NAME}/{JOB_AUDIT_TABLE_NAME}"

JOB_AUDIT_TABLE_NAME_SEC = f"{TENANT_NAME_SEC}.{STAGE_NAME}_{PRODUCT_NAME}_{JOB_AUDIT_TABLE}"
JOB_AUDIT_TABLE_LOC_SEC = f"{INT_MOUNT_POINT_SEC}/{STAGE_NAME}/{JOB_AUDIT_TABLE_NAME_SEC}"

TASK_AUDIT_TABLE = "task_audit"
TASK_AUDIT_TABLE_NAME = f"{TENANT_NAME}.{STAGE_NAME}_{PRODUCT_NAME}_{TASK_AUDIT_TABLE}"
TASK_AUDIT_TABLE_LOC = f"{INT_MOUNT_POINT}/{STAGE_NAME}/{TASK_AUDIT_TABLE_NAME}"

TASK_AUDIT_TABLE_NAME_SEC = f"{TENANT_NAME_SEC}.{STAGE_NAME}_{PRODUCT_NAME}_{TASK_AUDIT_TABLE}"
TASK_AUDIT_TABLE_LOC_SEC = f"{INT_MOUNT_POINT_SEC}/{STAGE_NAME}/{TASK_AUDIT_TABLE_NAME_SEC}"

class TestJobMonitoring(unittest.TestCase):

    @classmethod
    def get_dbutils(cls):
        spark = SparkSession.getActiveSession()
        return DBUtils(spark)

    @classmethod
    def setUpClass(self):
        print("Testing Job Monitoring script... setUpClass...")
        
        global_config = ConfigurationUtils.read_config(
            GlobalConfiguration.GLOBAL_JOBS_CONFIG_PATH
        )
        
        self.cluster_name = global_config["ClusterName"]
        print(f"Cluster name: {self.cluster_name}")

        self.dbutils = self.get_dbutils()

        ########### PREPARE PATH SAMPLE NOTEBOOK ########################################################

        self.context = json.loads(self.dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())

        self.current_path = self.context['extraContext']['notebook_path'].rsplit('/', 1)[0]
        print(f"Current_path: {self.current_path}")

        self.sample_notebook_full_path = f"{self.current_path}/{SAMPLE_NOTEBOOK_NAME}"
        print(f"Sample_notebook_full_path: {self.sample_notebook_full_path}")

        self.sample_notebook_full_path_not_exists = f"{self.current_path}/{SAMPLE_NOTEBOOK_NAME_NOT_EXISTS}"
        print(f"Sample_notebook_full_path_not_exists: {self.sample_notebook_full_path_not_exists}")

        ########### PREPARE PATH JOB MONITORING NOTEBOOK ################################################

        self.job_monitoring_path = self.context['extraContext']['notebook_path'].rsplit('/', 3)[0]
        print(f"Job_monitoring_path: {self.job_monitoring_path}")

        self.job_monitoring_full_path = f"{self.job_monitoring_path}/{JOB_MONITORIG_NOTEBOOK_NAME}"
        print(f"Job_monitoring_full_path: {self.job_monitoring_full_path}")

        ########### PREPARE TENANT_PRODUCT.JSON CONFIG ##################################################
        
        print("Create folders and tenant_product.json file...")
        tenant_product_json_data = json.dumps(TENANT_PRODUCT_CONFIG)
        self.dbutils.fs.mkdirs(ENV_TENANT_CONFIG_FOLDER)
        with open(ENV_TENANT_CONFIG_DBFS_PATH, 'w') as file:
            file.write(tenant_product_json_data)

        ########### CREATE DATABASE AND JOB_AUDIT AND TASK_AUDIT TABLES #################################
        
        print(f"Create {TENANT_NAME} database...")

        self.spark = SparkSession.getActiveSession()
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {TENANT_NAME} LOCATION '{INT_MOUNT_POINT}'")

        print(f"Create {JOB_AUDIT_TABLE_NAME} table...")
              
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {JOB_AUDIT_TABLE_NAME} (
            tenant STRING,
            product STRING,
            job_name STRING,
            job_id BIGINT,
            job_run_id BIGINT,
            job_start_time STRING,
            job_end_time STRING,
            job_run_page_url STRING,
            number_of_tasks BIGINT,
            job_status STRING,
            job_error_desc STRING,
            updated_by STRING,
            updated_at TIMESTAMP
        )
        USING DELTA
        LOCATION '{JOB_AUDIT_TABLE_LOC}';""")

        print(f"Creating {TASK_AUDIT_TABLE_NAME} table...")

        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {TASK_AUDIT_TABLE_NAME} (
            tenant STRING,
            product STRING,
            job_name STRING,
            job_id BIGINT,
            job_run_id BIGINT,
            task_name STRING,
            task_run_id BIGINT,
            task_start_time STRING,
            task_end_time STRING,
            task_run_page_url STRING,
            task_status STRING,
            task_error_desc STRING,
            updated_by STRING,
            updated_at TIMESTAMP
        )
        USING DELTA
        LOCATION '{TASK_AUDIT_TABLE_LOC}';""")

        ########### CREATE DATABASE AND JOB_AUDIT AND TASK_AUDIT TABLES - SECOND TENANT #################
        print(f"Creating {TENANT_NAME_SEC} database...")

        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {TENANT_NAME_SEC} LOCATION '{INT_MOUNT_POINT}'")

        print(f"Creating {JOB_AUDIT_TABLE_NAME_SEC} table...")
              
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {JOB_AUDIT_TABLE_NAME_SEC} (
            tenant STRING,
            product STRING,
            job_name STRING,
            job_id BIGINT,
            job_run_id BIGINT,
            job_start_time STRING,
            job_end_time STRING,
            job_run_page_url STRING,
            number_of_tasks BIGINT,
            job_status STRING,
            job_error_desc STRING,
            updated_by STRING,
            updated_at TIMESTAMP
        )
        USING DELTA
        LOCATION '{JOB_AUDIT_TABLE_LOC_SEC}';""")

        print(f"Creating {TASK_AUDIT_TABLE_NAME_SEC} table...")

        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {TASK_AUDIT_TABLE_NAME_SEC} (
            tenant STRING,
            product STRING,
            job_name STRING,
            job_id BIGINT,
            job_run_id BIGINT,
            task_name STRING,
            task_run_id BIGINT,
            task_start_time STRING,
            task_end_time STRING,
            task_run_page_url STRING,
            task_status STRING,
            task_error_desc STRING,
            updated_by STRING,
            updated_at TIMESTAMP
        )
        USING DELTA
        LOCATION '{TASK_AUDIT_TABLE_LOC_SEC}';""")

        print("Testing Job Monitoring script... setUpClass... DONE")


    def test_update_status_running_to_success(self):        
        print("Test name: test_update_status_running_to_success...")

        ########### CREATE TEST JOB WITH ONE TASK #################################

        print(f"Get cluster_id for cluster name {self.cluster_name}...")
        databricks_job_manager = DatabricksJobManager()
        self.cluster_id = databricks_job_manager.get_cluster_id(self.cluster_name)
        print(f"Cluster_id for cluster name {self.cluster_name} is: {self.cluster_id}")

        print(f"Creating sample task... Run notebook: {self.sample_notebook_full_path}")
        tasks_list = []

        task = databricks_job_manager.create_task(self.cluster_id, STAGE_NAME, TENANT_NAME, PRODUCT_NAME, SCHEMA_NAME, 
                                                  TABLE_NAME, TABLE_TYPE, PRIMARY_KEY, INCREMENTAL_COLUMN, DEPENDENT_TASK, self.sample_notebook_full_path)

        tasks_list.append(task)
        tasks_list_count = len(tasks_list)
        print(f"Number of tasks created: {tasks_list_count}.")

        print(f"Create sample job...")
        dts = datetime.now()
        self.job_name = f"{STAGE_NAME}_{TENANT_NAME}_{PRODUCT_NAME}_{dts}"
        job = DatabricksJobManager().create_job(self.job_name, tasks_list, STAGE_NAME, TENANT_NAME, PRODUCT_NAME)
        self.job_id = job.job_id

        ########### CREATE AUDIT ENTRY #######################################################
        
        print(f"Creating entries in job_audit and task_audit log tables for {self.job_name} with job_id {self.job_id}...")
        audit_logger = AuditLogger(TENANT_NAME, PRODUCT_NAME, STAGE_NAME, "insert")
        audit_logger.job_create_update(self.job_id)
        print(f"Creating entries in job_audit and task_audit log tables for {self.job_name} with job_id {self.job_id}... DONE")
        
        ########### RUN JOB #######################################################

        print(f"Starting job {self.job_name} with job_id {self.job_id}...")
        self.job_run_id = DatabricksJobManager().run_job(self.job_id)
        audit_logger = AuditLogger(TENANT_NAME, PRODUCT_NAME, STAGE_NAME, "run_id_update")
        audit_logger.job_run_update(self.job_run_id)
        print(f"Starting job {self.job_name} with job_id {self.job_id} and job_run_id {self.job_run_id}... DONE")

        ########### RUN JOB_MONITORING NOTEBOOK ###################################

        print(f"Notebook {self.job_monitoring_full_path} is running...")

        # wait for jobs to finish 
        time.sleep(30)

        with self.assertRaises(Exception) as run_job_monitoring:
            self.dbutils.notebook.run(self.job_monitoring_full_path, 120, {"layer": STAGE_NAME, "env_tenant_config_path": ENV_TENANT_CONFIG_PATH})

        time.sleep(30)
        print(f"Dropping job {self.job_name} with job_id {self.job_id}...")
        self.w = DatabricksJobManager.databricks_login()
        self.w.jobs.delete(self.job_id)

        ########### ASSERT JOB AND TASK STATUS ####################################

        print(f"Checking exception catcher for failing tenant.")

        self.assertIn("Caused by: com.databricks.NotebookExecutionException: FAILED: Workload failed, see run output for detail", str(run_job_monitoring.exception))

        print(f"Checking status of job {self.job_name} in job_audit table...")

        print(f"table location: {JOB_AUDIT_TABLE_LOC}, job_run_id: {self.job_run_id}")

        job_status_df = (
            self.spark.read.format("delta")
            .load(JOB_AUDIT_TABLE_LOC)
            .filter(f"job_run_id IN ({self.job_run_id})")
            .select("job_status")
        )

        actual_job_status = job_status_df.collect()[0][0]
        print(f"Status of job {self.job_name} is: {actual_job_status}")

        expected_job_status = "SUCCESS"
        self.assertEqual(expected_job_status, actual_job_status, msg="Job status is " + actual_job_status + " while expected status is " + expected_job_status + ".")


        print(f"table location: {TASK_AUDIT_TABLE_LOC}, job_run_id: {self.job_run_id}")
        task_status_df = (
            self.spark.read.format("delta")
            .load(TASK_AUDIT_TABLE_LOC)
            .filter(f"job_run_id IN ({self.job_run_id})")
            .select("task_status")
        )

        actual_task_status = task_status_df.collect()[0][0]
        print(f"Status of task in {self.job_name} is {actual_task_status}")

        expected_task_status = "SUCCESS"
        self.assertEqual(expected_task_status, actual_task_status, msg="Task status is " + actual_task_status + " while expected status is " + expected_task_status + ".")

        print("Test name: test_update_status_running_to_success... DONE")


    def test_update_status_running_to_failed_and_success(self):        
        print("Test name: test_update_status_running_to_failed_and_success...")

        ########### CREATE TEST JOB WITH ONE TASK #################################

        print(f"Get cluster_id for cluster name {self.cluster_name}...")
        databricks_job_manager = DatabricksJobManager()
        self.cluster_id = databricks_job_manager.get_cluster_id(self.cluster_name)
        print(f"Cluster_id for cluster name {self.cluster_name} is {self.cluster_id}")

        ### JOB for failure
        print(f"Creating sample task for failure... Run notebook: {self.sample_notebook_full_path_not_exists}")
        tasks_list = []

        task = databricks_job_manager.create_task(self.cluster_id, STAGE_NAME, TENANT_NAME, PRODUCT_NAME, SCHEMA_NAME, 
                                                  TABLE_NAME, TABLE_TYPE, PRIMARY_KEY, INCREMENTAL_COLUMN, DEPENDENT_TASK, self.sample_notebook_full_path_not_exists)

        tasks_list.append(task)
        
        tasks_list_count = len(tasks_list)
        print(f"Number of tasks created: {tasks_list_count}")

        print(f"Creating sample job for failure...")
        dts = datetime.now()
        self.job_name_1 = f"{STAGE_NAME}_{TENANT_NAME}_{PRODUCT_NAME}_{dts}"
        job = DatabricksJobManager().create_job(self.job_name_1, tasks_list, STAGE_NAME, TENANT_NAME, PRODUCT_NAME)
        self.job_id_1 = job.job_id

        ### JOB for success
        print(f"Creating sample task for success... Run notebook: {self.sample_notebook_full_path_not_exists}")
        tasks_list = []

        task = databricks_job_manager.create_task(self.cluster_id, STAGE_NAME, TENANT_NAME, PRODUCT_NAME, SCHEMA_NAME, 
                                                  TABLE_NAME, TABLE_TYPE, PRIMARY_KEY, INCREMENTAL_COLUMN, DEPENDENT_TASK, self.sample_notebook_full_path)

        tasks_list.append(task)
        
        tasks_list_count = len(tasks_list)
        print(f"Number of tasks created: {tasks_list_count}")

        print(f"Creating sample job for success...")
        dts = datetime.now()
        self.job_name_2 = f"{STAGE_NAME}_{TENANT_NAME}_{PRODUCT_NAME}_{dts}"
        job = DatabricksJobManager().create_job(self.job_name_2, tasks_list, STAGE_NAME, TENANT_NAME, PRODUCT_NAME)
        self.job_id_2 = job.job_id

        ########### CREATE AUDIT ENTRY #######################################################
        
        print(f"Creating entries in job_audit and task_audit log tables for {self.job_name_1} with job_id {self.job_id_1}...")
        audit_logger = AuditLogger(TENANT_NAME, PRODUCT_NAME, STAGE_NAME, "insert")
        audit_logger.job_create_update(self.job_id_1)
        print(f"Creating entries in job_audit and task_audit log tables for {self.job_name_1} with job_id {self.job_id_1}... DONE")

        print(f"Creating entries in job_audit and task_audit log tables for {self.job_name_2} with job_id {self.job_id_2}...")
        audit_logger = AuditLogger(TENANT_NAME, PRODUCT_NAME, STAGE_NAME, "insert")
        audit_logger.job_create_update(self.job_id_2)
        print(f"Creating entries in job_audit and task_audit log tables for {self.job_name_2} with job_id {self.job_id_2}... DONE")
        
        ########### RUN JOB #######################################################

        print(f"Starting job {self.job_name_1} with job_id {self.job_id_1}...")
        self.job_run_id_1 = DatabricksJobManager().run_job(self.job_id_1)
        audit_logger = AuditLogger(TENANT_NAME, PRODUCT_NAME, STAGE_NAME, "run_id_update")
        audit_logger.job_run_update(self.job_run_id_1)
        print(f"Starting job {self.job_name_1} with job_id {self.job_id_1} and job_run_id {self.job_run_id_1}... DONE")

        print(f"Starting job {self.job_name_2} with job_id {self.job_id_2}...")
        self.job_run_id_2 = DatabricksJobManager().run_job(self.job_id_2)
        audit_logger = AuditLogger(TENANT_NAME, PRODUCT_NAME, STAGE_NAME, "run_id_update")
        audit_logger.job_run_update(self.job_run_id_2)
        print(f"Starting job {self.job_name_2} with job_id {self.job_id_2} and job_run_id {self.job_run_id_2}... DONE")

        ########### RUN JOB_MONITORING NOTEBOOK ###################################

        # wait for jobs to finish 
        time.sleep(30)

        print(f"Notebook {self.job_monitoring_full_path} is running...")
        with self.assertRaises(Exception) as run_job_monitoring:
            self.dbutils.notebook.run(self.job_monitoring_full_path, 120, {"layer": STAGE_NAME, "env_tenant_config_path": ENV_TENANT_CONFIG_PATH})
            
        time.sleep(30)

        print(f"Dropping job {self.job_name_1} with job_id {self.job_id_1}...")
        self.w = DatabricksJobManager.databricks_login()
        self.w.jobs.delete(self.job_id_1)

        print(f"Dropping job {self.job_name_2} with job_id {self.job_id_2}...")
        self.w = DatabricksJobManager.databricks_login()
        self.w.jobs.delete(self.job_id_2)

        ########### ASSERT JOB AND TASK STATUS ####################################
        print(f"Checking exception catcher for failing tenant.")

        self.assertIn("Caused by: com.databricks.NotebookExecutionException: FAILED: Workload failed, see run output for detail", str(run_job_monitoring.exception))
        print(f"Checking status of job {self.job_name_1} & {self.job_name_2} in job_audit table...")

        job_status_df = (
            self.spark.read.format("delta")
            .load(JOB_AUDIT_TABLE_LOC)
            .filter(f"job_run_id IN ({self.job_run_id_1}, {self.job_run_id_2})")
            .select("job_run_id", "job_status")
        )

        actual_job_status = job_status_df.filter(f"job_run_id = {self.job_run_id_1}").select("job_status").collect()[0][0]
        print(f"Status of job {self.job_name_1} is {actual_job_status}")

        expected_job_status = "FAILED"
        self.assertEqual(expected_job_status, actual_job_status, msg="Job status is " + actual_job_status + " while expected status is " + expected_job_status + ".")

        actual_job_status = job_status_df.filter(f"job_run_id = {self.job_run_id_2}").select("job_status").collect()[0][0]
        print(f"Status of job {self.job_name_2} is {actual_job_status}")

        expected_job_status = "SUCCESS"
        self.assertEqual(expected_job_status, actual_job_status, msg="Job status is " + actual_job_status + " while expected status is " + expected_job_status + ".")

        task_status_df = (
            self.spark.read.format("delta")
            .load(TASK_AUDIT_TABLE_LOC)
            .filter(f"job_run_id IN ({self.job_run_id_1}, {self.job_run_id_2})")
            .select("job_run_id", "task_status")
        )
        
        actual_task_status = task_status_df.filter(f"job_run_id = {self.job_run_id_1}").select("task_status").collect()[0][0]
        print(f"Status of task in {self.job_name_1} is {actual_task_status}")

        expected_task_status = "FAILED"
        self.assertEqual(expected_task_status, actual_task_status, msg="Task status is " + actual_task_status + " while expected status is " + expected_task_status + ".")

        actual_task_status = task_status_df.filter(f"job_run_id = {self.job_run_id_2}").select("task_status").collect()[0][0]
        print(f"Status of task in {self.job_name_2} is {actual_task_status}")

        expected_task_status = "SUCCESS"
        self.assertEqual(expected_task_status, actual_task_status, msg="Task status is " + actual_task_status + " while expected status is " + expected_task_status + ".")

        print("Test name: test_update_status_running_to_failed_and_success... DONE")


    @classmethod
    def tearDownClass(cls):
        print("Testing Job Monitoring script... tearDownClass...")

        print(f"Dropping all databases...")
        cls.spark.sql(f"DROP DATABASE IF EXISTS {TENANT_NAME} CASCADE")
        cls.spark.sql(f"DROP DATABASE IF EXISTS {TENANT_NAME_SEC} CASCADE")

        print(f"Dropping all folders...")
        cls.dbutils.fs.rm(INT_MOUNT_POINT, True)
        cls.dbutils.fs.rm(INT_MOUNT_POINT_SEC, True)

        print("Testing Job Monitoring script... tearDownClass... DONE")