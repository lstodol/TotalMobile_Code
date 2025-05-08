import json
import unittest

from datetime import datetime
from pyspark.dbutils import DBUtils
from pyspark.sql import Row, SparkSession

INT_MOUNT_POINT = "/mnt/IntegrationTestJobRecovery"
JOB_RECOVERY_NOTEBOOK_NAME = "job_recovery"
JOB_ERROR_DESC_FAILED = "Job failed"
JOB_ERROR_DESC_AFTER_RECOVERY = "Changed status by job_recovery notebook"
JOB_STATUS_FAILED = "FAILED"
JOB_STATUS_AFTER_RECOVERY = "SUCCESS"
PRODUCT_NAME = "IntegrationTestJobRecoveryProduct"
STAGE_NAME = "bronze"
TABLE_NAME = "IntegrationTestJobRecoveryTable"
INCREMENTAL_COLUMN = "ValidFrom"
TENANT_NAME = "IntegrationTestJobRecovery"
JOB_AUDIT_TABLE = "job_audit"
JOB_AUDIT_TABLE_NAME = f"{TENANT_NAME}.{STAGE_NAME}_{PRODUCT_NAME}_{JOB_AUDIT_TABLE}"
JOB_AUDIT_TABLE_LOC = f"{INT_MOUNT_POINT}/{STAGE_NAME}/{JOB_AUDIT_TABLE_NAME}"


class TestJobRecovery(unittest.TestCase):

    @classmethod
    def get_dbutils(cls):
        spark = SparkSession.getActiveSession()
        return DBUtils(spark)

    @classmethod
    def setUpClass(self):
        print("Testing Job Recovery script... setUpClass...")

        self.dbutils = self.get_dbutils()

        ########### PREPARE PATH JOB RECOVER NOTEBOOK ###################################################

        self.context = json.loads(
            self.dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .toJson()
        )

        self.job_recovery_path = self.context["extraContext"]["notebook_path"].rsplit(
            "/", 3
        )[0]
        print(f"Job_recovery_path: {self.job_recovery_path}")

        self.job_recovery_full_path = (
            f"{self.job_recovery_path}/{JOB_RECOVERY_NOTEBOOK_NAME}"
        )
        print(f"Job_recovery_full_path: {self.job_recovery_full_path}")

        ########### CREATE DATABASE AND JOB_AUDIT TABLE #################################################

        print(f"Creating {TENANT_NAME} database...")

        self.spark = SparkSession.getActiveSession()
        self.spark.sql(
            f"CREATE DATABASE IF NOT EXISTS {TENANT_NAME} LOCATION '{INT_MOUNT_POINT}'"
        )

        print(f"Creating {JOB_AUDIT_TABLE_NAME} table...")

        self.spark.sql(
            f"""CREATE TABLE IF NOT EXISTS {JOB_AUDIT_TABLE_NAME} (
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
        LOCATION '{JOB_AUDIT_TABLE_LOC}';"""
        )

        print("Testing Job Recovery script... setUpClass... DONE")

    def test_job_recovery_status_to_success(self):
        print("Test name: test_job_recovery_status_to_success...")

        ########### INSERT ONE JOB DETAILS WITH STATUS FAILED #####################

        dts = datetime.now()

        df = self.spark.createDataFrame(
            [
                Row(
                    tenant=TENANT_NAME,
                    product=PRODUCT_NAME,
                    job_name=f"{STAGE_NAME}_{TENANT_NAME}_{PRODUCT_NAME}_{dts}",
                    job_id=100,
                    job_run_id=100,
                    job_start_time="",
                    job_end_time="",
                    job_run_page_url="",
                    number_of_tasks=5,
                    job_status=JOB_STATUS_FAILED,
                    job_error_desc=JOB_ERROR_DESC_FAILED,
                    updated_by="manual",
                    updated_at=datetime.now(),
                )
            ]
        )

        (
            df.write.format("delta")
            .option("mergeSchema", "true")
            .option("path", JOB_AUDIT_TABLE_LOC)
            .mode("append")
            .saveAsTable(JOB_AUDIT_TABLE_NAME)
        )

        ########### RUN JOB_RECOVERY NOTEBOOK #####################################

        print(f"Notebook {self.job_recovery_full_path} is running...")
        self.dbutils.notebook.run(
            self.job_recovery_full_path,
            60,
            {
                "layer": STAGE_NAME,
                "tenant": TENANT_NAME,
                "product": PRODUCT_NAME,
                "status": JOB_STATUS_AFTER_RECOVERY,
                "message": JOB_ERROR_DESC_AFTER_RECOVERY,
            },
        )

        ########### ASSERT JOB STATUS ############################################

        print(f"Checking status of job in job_audit table...")

        job_status_df = (
            self.spark.read.format("delta")
            .load(JOB_AUDIT_TABLE_LOC)
            .filter(f"tenant = '{TENANT_NAME}' and product = '{PRODUCT_NAME}'")
            .orderBy(f"job_name", ascending=False)
            .select("job_status")
        )

        actual_job_status = job_status_df.collect()[0][0]
        print(f"Status of job is {actual_job_status}")

        expected_job_status = "SUCCESS"
        self.assertEqual(
            expected_job_status,
            actual_job_status,
            msg="Job status is "
            + actual_job_status
            + " while expected status is "
            + expected_job_status
            + ".",
        )

        print("Test name: test_job_recovery_status_to_success... DONE")

    @classmethod
    def tearDownClass(cls):
        print("Testing Job Recovery script... tearDownClass...")

        print(f"Dropping {TENANT_NAME} database...")
        spark = SparkSession.getActiveSession()
        spark.sql(f"DROP DATABASE IF EXISTS {TENANT_NAME} CASCADE")

        print(f"Dropping {INT_MOUNT_POINT} folder...")
        dbutils = cls.get_dbutils()
        dbutils.fs.rm(INT_MOUNT_POINT, True)

        print("Testing Job Recovery script... tearDownClass... DONE")
