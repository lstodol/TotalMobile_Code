from unittest import TestCase
from datetime import datetime
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession, Row
from modules.databricks_utils import AuditLogger
from databricks.sdk.service import jobs
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType,
)


class TestAuditLogger(TestCase):
    @classmethod
    def append_row_in_job_audit_table(
        cls,
        tenant,
        product,
        layer,
        mount_location,
        job_id=1,
        job_run_id=1,
        job_name="jobname",
        job_start_time=datetime.now(),
        job_end_time=datetime.now(),
        job_status="SUCCESS",
        schema="dbo",
    ):
        job_audit_table_name = f"{tenant}.{layer}_{product}_job_audit"
        # job_audit_table_loc = f"{mount_location}/{layer}/{cls.job_audit_table_name}"

        if job_status == "CREATED":
            df = cls.spark.createDataFrame(
                [
                    Row(
                        tenant=tenant,
                        product=product,
                        job_name=job_name,
                        job_id=job_id,
                        number_of_tasks=1,
                        job_status=job_status,
                        updated_by="TESTUSER",
                        updated_at=datetime.now(),
                    )
                ]
            )
        else:
            df = cls.spark.createDataFrame(
                [
                    Row(
                        tenant=tenant,
                        product=product,
                        job_name=job_name,
                        job_id=job_id,
                        job_run_id=job_run_id,
                        job_start_time=job_start_time,
                        job_end_time=job_end_time,
                        job_run_page_url="NA",
                        number_of_tasks=1,
                        job_status=job_status,
                        job_error_desc="NO ERROR",
                        updated_by="TESTUSER",
                        updated_at=datetime.now(),
                    )
                ]
            )

        (
            df.write.format("delta")
            .option("mergeSchema", "true")
            .mode("append")
            .saveAsTable(job_audit_table_name)
        )

    @classmethod
    def append_row_in_task_audit_table(
        cls,
        tenant,
        product,
        layer,
        mount_location,
        job_id=1,
        job_run_id=1,
        job_name="jobname",
        task_run_id=1,
        task_name="taskname",
        task_start_time=datetime.now(),
        job_status="RUNNING",
    ):
        task_audit_table_name = f"{tenant}.{layer}_{product}_task_audit"
        # task_audit_table_loc = f"{mount_location}/{layer}/{cls.task_audit_table_name}"

        if job_status == "CREATED":
            df = cls.spark.createDataFrame(
                [
                    Row(
                        tenant=tenant,
                        product=product,
                        job_name=job_name,
                        job_id=job_id,
                        task_name=task_name,
                        task_status=job_status,
                        updated_by="TESTUSER",
                        updated_at=datetime.now(),
                    )
                ]
            )
        else:
            # create task audit table
            df = cls.spark.createDataFrame(
                [
                    Row(
                        tenant=tenant,
                        product=product,
                        job_name=job_name,
                        job_id=job_id,
                        job_run_id=job_run_id,
                        task_name=task_name,
                        task_run_id=task_run_id,
                        task_start_time=task_start_time,
                        # task_end_time=datetime.now(),
                        # task_run_page_url="NA",
                        task_status="RUNNING",
                        # task_error_desc="NO ERROR",
                        updated_by="TESTUSER",
                        updated_at=datetime.now(),
                    )
                ]
            )
        (
            df.write.format("delta")
            .option("mergeSchema", "true")
            .mode("append")
            .saveAsTable(task_audit_table_name)
        )

    @classmethod
    def setUpClass(cls):
        print("Testing TestAuditLogger - setUpClass...")

        cls.spark = SparkSession.builder.getOrCreate()
        cls.dbutils = DBUtils(cls.spark)
        cls.tenant = "integrationauditloggertest"
        cls.product = "carelinktest"
        cls.layer = "bronze"
        cls.schema = "dbo"
        cls.mount_location = f"/mnt/{cls.tenant}"
        cls.dbutils.fs.mkdirs(cls.mount_location)

        job_audit_schema = StructType(
            [
                StructField("tenant", StringType(), True),
                StructField("product", StringType(), True),
                StructField("job_name", StringType(), True),
                StructField("job_id", LongType(), True),
                StructField("job_run_id", LongType(), True),
                StructField("job_start_time", TimestampType(), True),
                StructField("job_end_time", TimestampType(), True),
                StructField("job_run_page_url", StringType(), True),
                StructField("number_of_tasks", LongType(), True),
                StructField("job_status", StringType(), True),
                StructField("job_error_desc", StringType(), True),
                StructField("updated_by", StringType(), True),
                StructField("updated_at", TimestampType(), True),
            ]
        )

        task_audit_schema = StructType(
            [
                StructField("tenant", StringType(), True),
                StructField("product", StringType(), True),
                StructField("job_name", StringType(), True),
                StructField("job_id", LongType(), True),
                StructField("job_run_id", LongType(), True),
                StructField("task_name", StringType(), True),
                StructField("task_run_id", LongType(), True),
                StructField("task_start_time", TimestampType(), True),
                StructField("task_end_time", TimestampType(), True),
                StructField("task_run_page_url", StringType(), True),
                StructField("task_status", StringType(), True),
                StructField("task_error_desc", StringType(), True),
                StructField("updated_by", StringType(), True),
                StructField("updated_at", TimestampType(), True),
            ]
        )

        cls.spark.sql(f"DROP DATABASE IF EXISTS {cls.tenant} CASCADE")
        cls.spark.sql(
            f"CREATE DATABASE IF NOT EXISTS {cls.tenant} LOCATION '{cls.mount_location}'"
        )

        cls.job_audit_table_name = f"{cls.tenant}.{cls.layer}_{cls.product}_job_audit"
        cls.job_audit_table_loc = (
            f"{cls.mount_location}/{cls.layer}/{cls.job_audit_table_name}"
        )
        cls.spark.catalog.createTable(
            cls.job_audit_table_name,
            path=cls.job_audit_table_loc,
            schema=job_audit_schema,
        )

        cls.task_audit_table_name = f"{cls.tenant}.{cls.layer}_{cls.product}_task_audit"
        cls.task_audit_table_loc = (
            f"{cls.mount_location}/{cls.layer}/{cls.task_audit_table_name}"
        )
        cls.spark.catalog.createTable(
            cls.task_audit_table_name,
            path=cls.task_audit_table_loc,
            schema=task_audit_schema,
        )

        cls.job_run_output = jobs.Run(
            job_id=1,
            run_id=1,
            run_name="runname",
            tasks=[
                jobs.RunTask(run_id=2),
                jobs.RunTask(run_id=3),
                jobs.RunTask(run_id=3),
            ],
            run_page_url="https://runpageurl.com",
            start_time=1701422922910,
            end_time=1701423922910,
            state=jobs.RunState(
                life_cycle_state=jobs.RunLifeCycleState.TERMINATED,
                queue_reason=None,
                result_state=jobs.RunResultState.SUCCESS,
                state_message="state message",
                user_cancelled_or_timedout=False,
            ),
        )

        cls.run_output = jobs.RunOutput(metadata=cls.job_run_output)

    def setUp(self):
        pass

    def tearDown(self):
        print("Tearing it down...")

        job_audit_table_name = f"{self.layer}_{self.product}_job_audit"
        if self.spark._jsparkSession.catalog().tableExists(
            self.tenant, job_audit_table_name
        ):
            print("job audit exists...deleting")
            self.spark.sql(f"delete from {self.job_audit_table_name}")

        task_audit_table_name = f"{self.layer}_{self.product}_task_audit"
        if self.spark._jsparkSession.catalog().tableExists(
            self.tenant, task_audit_table_name
        ):
            print("task audit exists...deleting")
            self.spark.sql(f"delete from {self.task_audit_table_name}")

    def test_get_active_job_no_records(self):
        # arrange
        job_run_id = 1000
        self.append_row_in_job_audit_table(
            self.tenant,
            self.product,
            self.layer,
            self.mount_location,
            job_run_id=job_run_id,
        )
        audit_logger = AuditLogger(self.tenant, self.product, self.layer, "some_action")

        # act
        df = audit_logger.get_active_jobs()

        # assert
        self.assertEqual(
            df.filter(f"job_run_id = {job_run_id}").count(),
            0,
            msg="Audit logger is returning wrong jobs as active.",
        )

    def test_get_active_job_queued(self):
        # arrange
        job_run_id = 1001
        self.append_row_in_job_audit_table(
            self.tenant,
            self.product,
            self.layer,
            self.mount_location,
            job_run_id=job_run_id,
            job_status="QUEUED",
        )
        audit_logger = AuditLogger(self.tenant, self.product, self.layer, "some_action")

        # act
        df = audit_logger.get_active_jobs()

        # assert
        self.assertEqual(
            df.filter(f"job_run_id = {job_run_id}").count(),
            1,
            msg="Audit logger is not returning QUEUED job as active.",
        )

    def test_get_active_job_pending(self):
        # arrange
        job_run_id = 1002
        self.append_row_in_job_audit_table(
            self.tenant,
            self.product,
            self.layer,
            self.mount_location,
            job_run_id=job_run_id,
            job_status="PENDING",
        )
        audit_logger = AuditLogger(self.tenant, self.product, self.layer, "some_action")

        # act
        df = audit_logger.get_active_jobs()

        # assert
        self.assertEqual(
            df.filter(f"job_run_id = {job_run_id}").count(),
            1,
            msg="Audit logger is not returning PENDING job as active.",
        )

    def test_get_active_job_running(self):
        # arrange
        job_run_id = 1003
        self.append_row_in_job_audit_table(
            self.tenant,
            self.product,
            self.layer,
            self.mount_location,
            job_run_id=job_run_id,
            job_status="RUNNING",
        )
        audit_logger = AuditLogger(self.tenant, self.product, self.layer, "some_action")

        # act
        df = audit_logger.get_active_jobs()

        # assert
        self.assertEqual(
            df.filter(f"job_run_id = {job_run_id}").count(),
            1,
            msg="Audit logger is not returning RUNNING job as active.",
        )

    def test_get_active_job_terminating(self):
        # arrange
        job_run_id = 1004
        self.append_row_in_job_audit_table(
            self.tenant,
            self.product,
            self.layer,
            self.mount_location,
            job_run_id=job_run_id,
            job_status="TERMINATING",
        )
        audit_logger = AuditLogger(self.tenant, self.product, self.layer, "some_action")

        # act
        df = audit_logger.get_active_jobs()

        # assert
        self.assertEqual(
            df.filter(f"job_run_id = {job_run_id}").count(),
            1,
            msg="Audit logger is not returning TERMINATING job as active.",
        )

    def test_read_job_audit_table_first_run(self):
        # arrange
        expected_status = "FIRST_RUN"
        audit_logger = AuditLogger(self.tenant, self.product, self.layer, "some_action")
        # act
        job_status = audit_logger.read_job_audit_table()

        # assert
        self.assertEqual(
            expected_status,
            job_status,
            msg=f"Wrong status returned: {job_status}.Expected status is: {expected_status}.",
        )

    def test_read_job_audit_table_with_status(self):
        # arrange
        self.append_row_in_job_audit_table(
            self.tenant,
            self.product,
            self.layer,
            self.mount_location,
            job_run_id=2000,
            job_status="RUNNING",
        )
        excepted_status = "RUNNING"
        audit_logger = AuditLogger(self.tenant, self.product, self.layer, "some_action")

        # act
        job_status = audit_logger.read_job_audit_table()

        # assert
        self.assertEqual(
            excepted_status,
            job_status,
            msg=f"Wrong status returned: {job_status}. Expected status is: {excepted_status}.",
        )

    def test_upd_job_audit(self):
        # arrange
        job_id = 10
        run_id = 1006
        run_name = f"jobanme_{job_id}_{run_id}"
        run_page_url = "https://runpageurl.com"
        start_time = datetime.fromisoformat("2023-12-01 09:28:42.910")
        end_time = datetime.fromisoformat("2023-12-07 09:28:42.912")
        updated_at = datetime.fromisoformat("2023-12-07 09:29:42.910")
        updated_by = "inttest-unity-app"
        status = "TERMINATED"
        error_desc = "some error"

        current_datetime = datetime.now()
        self.append_row_in_job_audit_table(
            self.tenant,
            self.product,
            self.layer,
            self.mount_location,
            job_id=job_id,
            job_run_id=run_id,
            job_name=run_name,
            job_start_time=start_time,
            job_status="RUNNING",
        )

        run_details = {
            "run_id": run_id,
            "run_name": run_name,
            "run_page_url": run_page_url,
            "updated_by": updated_by,
            "updated_at": updated_at,
            "start_time": start_time,
            "end_time": end_time,
            "status": status,
            "error_desc": error_desc,
        }

        audit_logger = AuditLogger(self.tenant, self.product, self.layer, "update")

        # act
        audit_logger.upd_job_audit(run_details=run_details)

        job_audit = (
            self.spark.read.table(self.job_audit_table_name)
            .filter(f"job_run_id = {run_id}")
            .collect()
        )

        self.assertEqual(
            1, len(job_audit), msg="Too many or no records for tested job audit record."
        )
        job_audit = job_audit[0]

        self.assertEqual(
            job_id,
            job_audit.job_id,
            msg=f"Wrong job_id - expected: {job_id}, but got: {job_audit.job_id}.",
        )
        self.assertEqual(
            run_name,
            job_audit.job_name,
            msg=f"Wrong job_name - expected: {run_name}, but got: {job_audit.job_name}.",
        )
        self.assertEqual(
            run_id,
            job_audit.job_run_id,
            msg=f"Wrong job_run_id - expected: {run_id}, but got: {job_audit.job_run_id}.",
        )
        self.assertEqual(
            start_time,
            job_audit.job_start_time,
            msg=f"Wrong end_time - expected: {start_time}, but got: {job_audit.job_start_time}.",
        )
        self.assertEqual(
            end_time,
            job_audit.job_end_time,
            msg=f"Wrong end_time - expected: {end_time}, but got: {job_audit.job_end_time}.",
        )
        self.assertEqual(
            status,
            job_audit.job_status,
            msg=f"Wrong status - expected: {status}, but got: {job_audit.job_status}.",
        )
        self.assertEqual(
            error_desc,
            job_audit.job_error_desc,
            msg=f"Wrong job_error_desc - expected: {error_desc}, but got: {job_audit.job_error_desc}.",
        )
        self.assertEqual(
            updated_by,
            job_audit.updated_by,
            msg=f"Wrong updated_by - expected: {updated_by}, but got: {job_audit.updated_by}.",
        )
        self.assertGreaterEqual(
            job_audit.updated_at,
            current_datetime,
            msg=f"Wrong updated_at - expected time to be greater than: {str(current_datetime)}, but got: {job_audit.updated_at}.",
        )

    def test_run_id_upd_job_audit(self):
        # arrange
        job_id = 11
        run_id = 1007
        run_name = f"jobanme_{job_id}_{run_id}"
        run_page_url = "https://runpageurl.com"
        number_of_tasks = 1
        start_time = datetime.fromisoformat("2023-12-01 09:28:42.910")
        updated_at = datetime.fromisoformat("2023-12-07 09:29:42.910")
        end_time = datetime.fromisoformat("2023-12-07 09:29:41.910")
        updated_by = "inttest-unity-app"
        status = "RUNNING"
        error_desc = ""
        current_datetime = datetime.now()

        self.append_row_in_job_audit_table(
            self.tenant,
            self.product,
            self.layer,
            self.mount_location,
            job_id=job_id,
            job_name=run_name,
            job_status="CREATED",
        )

        run_details = {
            "job_id": job_id,
            "run_id": run_id,
            "run_name": run_name,
            "run_page_url": run_page_url,
            "number_of_tasks": number_of_tasks,
            "start_time": start_time,
            "updated_by": updated_by,
            "updated_at": updated_at,
            "end_time": end_time,
            "status": status,
            "error_desc": error_desc,
        }

        audit_logger = AuditLogger(
            self.tenant, self.product, self.layer, "run_id_update"
        )

        # act
        audit_logger.run_id_upd_job_audit(run_details=run_details)

        job_audit = (
            self.spark.read.table(self.job_audit_table_name)
            .filter(f"job_id = {job_id}")
            .collect()
        )

        self.assertEqual(
            1, len(job_audit), msg="Too many or no records for tested job audit record."
        )
        job_audit = job_audit[0]

        self.assertEqual(
            job_id,
            job_audit.job_id,
            msg=f"Wrong job_id - expected: {job_id}, but got: {job_audit.job_id}.",
        )
        self.assertEqual(
            run_name,
            job_audit.job_name,
            msg=f"Wrong job_name - expected: {run_name}, but got: {job_audit.job_name}.",
        )
        self.assertEqual(
            run_id,
            job_audit.job_run_id,
            msg=f"Wrong job_run_id - expected: {run_id}, but got: {job_audit.job_run_id}.",
        )
        self.assertEqual(
            end_time,
            job_audit.job_end_time,
            msg=f"Wrong end_time - expected: {end_time}, but got: {job_audit.job_end_time}.",
        )
        self.assertEqual(
            start_time,
            job_audit.job_start_time,
            msg=f"Wrong start_time - expected: {start_time}, but got: {job_audit.job_start_time}.",
        )
        self.assertEqual(
            status,
            job_audit.job_status,
            msg=f"Wrong status - expected: {status}, but got: {job_audit.job_status}.",
        )
        self.assertEqual(
            error_desc,
            job_audit.job_error_desc,
            msg=f"Wrong job_error_desc - expected: {error_desc}, but got: {job_audit.job_error_desc}.",
        )
        self.assertEqual(
            updated_by,
            job_audit.updated_by,
            msg=f"Wrong updated_by - expected: {updated_by}, but got: {job_audit.updated_by}.",
        )
        self.assertGreaterEqual(
            job_audit.updated_at,
            current_datetime,
            msg=f"Wrong updated_at - expected time to be greater than: {str(current_datetime)}, but got: {job_audit.updated_at}.",
        )

    def test_upd_task_audit(self):
        # arrange
        job_id = 12
        run_id = 1008
        run_name = f"jobname_{job_id}_{run_id}"
        task_run_id = 1009
        task_name = f"taskname_{task_run_id}"

        current_datetime = datetime.now()
        updated_at = datetime.fromisoformat("2023-12-07 09:29:42.910")
        start_time = datetime.fromisoformat("2023-12-01 09:28:42.910")
        end_time = datetime.fromisoformat("2023-12-07 09:28:42.912")
        run_page_url = "https://runpageurl.com"
        updated_by = "inttest-unity-app"
        status = "TERMINATED"
        error_desc = "some error"

        self.append_row_in_task_audit_table(
            self.tenant,
            self.product,
            self.layer,
            self.mount_location,
            job_id=job_id,
            job_run_id=run_id,
            job_name=run_name,
            task_run_id=task_run_id,
            task_name=task_name,
            task_start_time=start_time,
        )

        run_details = {
            "job_id": job_id,
            "job_name": run_name,
            "job_run_id": run_id,
            "run_id": task_run_id,
            "updated_by": updated_by,
            "updated_at": updated_at,
            "start_time": start_time,
            "end_time": end_time,
            "status": status,
            "run_page_url": run_page_url,
            "error_desc": error_desc,
            "run_name": task_name,
        }

        audit_logger = AuditLogger(self.tenant, self.product, self.layer, "update")

        # act
        audit_logger.update_task_audit_table([run_details])

        task_audit = (
            self.spark.read.table(self.task_audit_table_name)
            .filter(f"task_run_id == {task_run_id}")
            .collect()
        )

        self.assertEqual(
            1,
            len(task_audit),
            msg="Too many or no records for tested task audit record.",
        )
        task_audit = task_audit[0]

        self.assertEqual(
            job_id,
            task_audit.job_id,
            msg=f"Wrong job_id - expected: {job_id}, but got: {task_audit.job_id}.",
        )

        self.assertEqual(
            run_id,
            task_audit.job_run_id,
            msg=f"Wrong job_run_id - expected: {run_id}, but got: {task_audit.job_run_id}.",
        )
        self.assertEqual(
            run_name,
            task_audit.job_name,
            msg=f"Wrong job_name - expected: {run_name}, but got: {task_audit.job_name}.",
        )

        self.assertEqual(
            task_run_id,
            task_audit.task_run_id,
            msg=f"Wrong task_run_id - expected: {task_run_id}, but got: {task_audit.task_run_id}.",
        )
        self.assertEqual(
            task_name,
            task_audit.task_name,
            msg=f"Wrong task_name - expected: {task_name}, but got: {task_audit.task_name}.",
        )
        self.assertEqual(
            start_time,
            task_audit.task_start_time,
            msg=f"Wrong task_start_time - expected: {start_time}, but got: {task_audit.task_start_time}.",
        )
        self.assertEqual(
            end_time,
            task_audit.task_end_time,
            msg=f"Wrong task_end_time - expected: {end_time}, but got: {task_audit.task_end_time}.",
        )
        self.assertEqual(
            status,
            task_audit.task_status,
            msg=f"Wrong task_status - expected: {status}, but got: {task_audit.task_status}.",
        )
        self.assertEqual(
            error_desc,
            task_audit.task_error_desc,
            msg=f"Wrong task_error_desc - expected: {error_desc}, but got: {task_audit.task_error_desc}.",
        )
        self.assertEqual(
            run_page_url,
            task_audit.task_run_page_url,
            msg=f"Wrong run_page_url - expected: {run_page_url}, but got: {task_audit.task_run_page_url}.",
        )
        self.assertEqual(
            updated_by,
            task_audit.updated_by,
            msg=f"Wrong updated_by - expected: {updated_by}, but got: {task_audit.updated_by}.",
        )
        self.assertGreaterEqual(
            task_audit.updated_at,
            current_datetime,
            msg=f"Wrong updated_at - expected time to be greater than: {current_datetime}, but got: {task_audit.updated_at}.",
        )

    def test_upd_task_audit_with_retry_task(self):
        # arrange
        job_id = 13
        run_id = 1018
        run_name = f"jobname_{job_id}_{run_id}"
        task_run_id_1 = 1019
        task_name = f"taskname_{job_id}_{run_id}"
        task_run_id_2 = 1020

        current_datetime = datetime.now()
        updated_by = "inttest-unity-app"
        status = "TERMINATED"

        updated_at_1 = datetime.fromisoformat("2023-12-07 09:29:42.910000")
        start_time_1 = datetime.fromisoformat("2023-12-01 09:28:42.910000")
        end_time_1 = datetime.fromisoformat("2023-12-07 09:28:42.912000")
        run_page_url_1 = "https://runpageurl_1.com"
        error_desc_1 = "some error 1"

        updated_at_2 = datetime.fromisoformat("2023-12-07 09:50:42.910000")
        start_time_2 = datetime.fromisoformat("2023-12-07 09:30:42.910000")
        end_time_2 = datetime.fromisoformat("2023-12-07 09:40:42.912000")
        run_page_url_2 = "https://runpageurl_2.com"
        error_desc_2 = "some error 2"

        self.append_row_in_task_audit_table(
            self.tenant,
            self.product,
            self.layer,
            self.mount_location,
            job_id=job_id,
            job_run_id=run_id,
            job_name=run_name,
            task_run_id=task_run_id_1,
            task_name=task_name,
            task_start_time=start_time_1,
        )

        run_details = [
            {
                "job_id": job_id,
                "job_name": run_name,
                "job_run_id": run_id,
                "run_id": task_run_id_1,
                "updated_by": updated_by,
                "updated_at": updated_at_1,
                "start_time": start_time_1,
                "end_time": end_time_1,
                "run_page_url": run_page_url_1,
                "status": status,
                "error_desc": error_desc_1,
                "run_name": task_name,
            },
            {
                "job_id": job_id,
                "job_name": run_name,
                "job_run_id": run_id,
                "run_id": task_run_id_2,
                "updated_by": updated_by,
                "updated_at": updated_at_2,
                "start_time": start_time_2,
                "end_time": end_time_2,
                "run_page_url": run_page_url_2,
                "status": status,
                "error_desc": error_desc_2,
                "run_name": task_name,
            },
        ]

        audit_logger = AuditLogger(self.tenant, self.product, self.layer, "update")

        # act
        audit_logger.update_task_audit_table(run_details)

        task_audit_1 = (
            self.spark.read.table(self.task_audit_table_name)
            .filter(f"task_run_id == {task_run_id_1}")
            .collect()
        )

        self.assertEqual(
            1,
            len(task_audit_1),
            msg="Too many or no records for tested task audit record.",
        )
        task_audit_1 = task_audit_1[0]

        self.assertEqual(
            job_id,
            task_audit_1.job_id,
            msg=f"Wrong job_id - expected: {job_id}, but got: {task_audit_1.job_id}.",
        )

        self.assertEqual(
            run_id,
            task_audit_1.job_run_id,
            msg=f"Wrong job_run_id - expected: {run_id}, but got: {task_audit_1.job_run_id}.",
        )
        self.assertEqual(
            run_name,
            task_audit_1.job_name,
            msg=f"Wrong job_name - expected: {run_name}, but got: {task_audit_1.job_name}.",
        )
        self.assertEqual(
            task_run_id_1,
            task_audit_1.task_run_id,
            msg=f"Wrong task_run_id - expected: {task_run_id_1}, but got: {task_audit_1.task_run_id}.",
        )
        self.assertEqual(
            task_name,
            task_audit_1.task_name,
            msg=f"Wrong task_name - expected: {task_name}, but got: {task_audit_1.task_name}.",
        )
        self.assertEqual(
            start_time_1,
            task_audit_1.task_start_time,
            msg=f"Wrong task_start_time - expected: {start_time_1}, but got: {task_audit_1.task_start_time}.",
        )
        self.assertEqual(
            end_time_1,
            task_audit_1.task_end_time,
            msg=f"Wrong task_end_time - expected: {end_time_1}, but got: {task_audit_1.task_end_time}.",
        )
        self.assertEqual(
            status,
            task_audit_1.task_status,
            msg=f"Wrong task_status - expected: {status}, but got: {task_audit_1.task_status}.",
        )
        self.assertEqual(
            error_desc_1,
            task_audit_1.task_error_desc,
            msg=f"Wrong task_error_desc - expected: {error_desc_1}, but got: {task_audit_1.task_error_desc}.",
        )
        self.assertEqual(
            updated_by,
            task_audit_1.updated_by,
            msg=f"Wrong updated_by - expected: {updated_by}, but got: {task_audit_1.updated_by}.",
        )
        self.assertEqual(
            run_page_url_1,
            task_audit_1.task_run_page_url,
            msg=f"Wrong run_page_url - expected: {run_page_url_1}, but got: {task_audit_1.task_run_page_url}.",
        )
        self.assertGreaterEqual(
            task_audit_1.updated_at,
            current_datetime,
            msg=f"Wrong updated_at - expected time to be greater than: {current_datetime}, but got: {task_audit_1.updated_at}.",
        )

        task_audit_2 = (
            self.spark.read.table(self.task_audit_table_name)
            .filter(f"task_run_id == {task_run_id_2}")
            .collect()
        )

        self.assertEqual(
            1,
            len(task_audit_2),
            msg="Too many or no records for tested task audit record.",
        )
        task_audit_2 = task_audit_2[0]

        self.assertEqual(
            job_id,
            task_audit_2.job_id,
            msg=f"Wrong job_id - expected: {job_id}, but got: {task_audit_2.job_id}.",
        )

        self.assertEqual(
            run_id,
            task_audit_2.job_run_id,
            msg=f"Wrong job_run_id - expected: {run_id}, but got: {task_audit_2.job_run_id}.",
        )
        self.assertEqual(
            run_name,
            task_audit_2.job_name,
            msg=f"Wrong job_name - expected: {run_name}, but got: {task_audit_2.job_name}.",
        )
        self.assertEqual(
            task_run_id_2,
            task_audit_2.task_run_id,
            msg=f"Wrong task_run_id - expected: {task_run_id_2}, but got: {task_audit_2.task_run_id}.",
        )
        self.assertEqual(
            task_name,
            task_audit_2.task_name,
            msg=f"Wrong task_name - expected: {task_name}, but got: {task_audit_2.task_name}.",
        )
        self.assertEqual(
            start_time_2,
            task_audit_2.task_start_time,
            msg=f"Wrong task_start_time - expected: {start_time_2}, but got: {task_audit_2.task_start_time}.",
        )
        self.assertEqual(
            end_time_2,
            task_audit_2.task_end_time,
            msg=f"Wrong task_end_time - expected: {end_time_2}, but got: {task_audit_2.task_end_time}.",
        )
        self.assertEqual(
            status,
            task_audit_2.task_status,
            msg=f"Wrong task_status - expected: {status}, but got: {task_audit_2.task_status}.",
        )
        self.assertEqual(
            error_desc_2,
            task_audit_2.task_error_desc,
            msg=f"Wrong task_error_desc - expected: {error_desc_2}, but got: {task_audit_2.task_error_desc}.",
        )
        self.assertEqual(
            updated_by,
            task_audit_2.updated_by,
            msg=f"Wrong updated_by - expected: {updated_by}, but got: {task_audit_2.updated_by}.",
        )
        self.assertEqual(
            run_page_url_2,
            task_audit_2.task_run_page_url,
            msg=f"Wrong run_page_url - expected: {run_page_url_2}, but got: {task_audit_2.task_run_page_url}.",
        )
        self.assertGreaterEqual(
            task_audit_2.updated_at,
            current_datetime,
            msg=f"Wrong updated_at - expected time to be greater than: {current_datetime}, but got: {task_audit_2.updated_at}.",
        )

    def test_run_id_upd_task_audit(self):
        # arrange
        job_id = 13
        run_name = f"jobname_{job_id}"
        task_name = f"taskname_{job_id}"

        run_id = 1028
        task_run_id = 1029

        current_datetime = datetime.now()
        updated_at = datetime.fromisoformat("2023-12-07 09:29:42.910000")
        start_time = datetime.fromisoformat("2023-12-01 09:28:42.910000")
        end_time = datetime.fromisoformat("2023-12-07 09:29:40.910000")
        run_page_url = "https://runpageurl.com"
        updated_by = "inttest-unity-app"
        status = "RUNNING"
        number_of_tasks = 1
        error_desc = ""
        current_datetime = datetime.now()

        self.append_row_in_task_audit_table(
            self.tenant,
            self.product,
            self.layer,
            self.mount_location,
            job_id=job_id,
            job_name=run_name,
            task_name=task_name,
            job_status="CREATED",
        )

        run_details = {
            "job_id": job_id,
            "job_run_id": run_id,
            "job_name": run_name,
            "run_id": task_run_id,
            "run_name": task_name,
            "run_page_url": run_page_url,
            "start_time": start_time,
            "updated_by": updated_by,
            "updated_at": updated_at,
            "end_time": end_time,
            "status": status,
            "error_desc": error_desc,
        }

        audit_logger = AuditLogger(
            self.tenant, self.product, self.layer, "run_id_update"
        )

        # act
        audit_logger.update_task_audit_table([run_details])

        task_audit = (
            self.spark.read.table(self.task_audit_table_name)
            .filter(f"job_id = {job_id} and task_name = '{task_name}'")
            .collect()
        )

        self.assertEqual(
            1,
            len(task_audit),
            msg="Too many or no records for tested job audit record.",
        )
        task_audit = task_audit[0]

        self.assertEqual(
            job_id,
            task_audit.job_id,
            msg=f"Wrong job_id - expected: {job_id}, but got: {task_audit.job_id}.",
        )

        self.assertEqual(
            run_id,
            task_audit.job_run_id,
            msg=f"Wrong job_run_id - expected: {run_id}, but got: {task_audit.job_run_id}",
        )
        self.assertEqual(
            run_name,
            task_audit.job_name,
            msg=f"Wrong job_name - expected: {run_name}, but got: {task_audit.job_name}",
        )
        self.assertEqual(
            task_run_id,
            task_audit.task_run_id,
            msg=f"Wrong task_run_id - expected: {task_run_id}, but got: {task_audit.task_run_id}",
        )
        self.assertEqual(
            task_name,
            task_audit.task_name,
            msg=f"Wrong task_name - expected: {task_name}, but got: {task_audit.task_name}",
        )
        self.assertEqual(
            end_time,
            
            task_audit.task_end_time,
            msg=f"Wrong end_time - expected: {end_time}, but got: {task_audit.task_end_time}",
        )
        self.assertEqual(
            start_time,
            task_audit.task_start_time,
            msg=f"Wrong start_time - expected: {start_time}, but got: {task_audit.task_start_time}",
        )
        self.assertEqual(
            status,
            task_audit.task_status,
            msg=f"Wrong status - expected: {status}, but got: {task_audit.task_status}",
        )
        self.assertEqual(
            error_desc,
            task_audit.task_error_desc,
            msg=f"Wrong task_error_desc - expected: {error_desc}, but got: {task_audit.task_error_desc}",
        )
        self.assertEqual(
            updated_by,
            task_audit.updated_by,
            msg=f"Wrong updated_by - expected: {updated_by}, but got: {task_audit.updated_by}",
        )
        self.assertEqual(
            run_page_url,
            task_audit.task_run_page_url,
            msg=f"Wrong run_page_url - expected: {run_page_url}, but got: {task_audit.task_run_page_url}.",
        )
        self.assertGreaterEqual(
            task_audit.updated_at,
            current_datetime,
            msg=f"Wrong updated_at - expected time to be greater than: {str(current_datetime)}, but got: {task_audit.updated_at}",
        )

    def test_insert_job_audit_table(self):
        # arrange
        job_id = 10007
        job_name = f"jobname_{job_id}"
        run_page_url = "https://runpageurlinsert.com"
        start_time = datetime.fromisoformat("2023-12-04 00:05:23.283000")
        updated_by = "inttest-unity-app"
        end_time = datetime.fromisoformat("2023-12-07 09:28:42.912345")
        status = "TERMINATED"
        error_desc = "some error with desc"
        number_of_tasks = 4

        current_datetime = datetime.now()

        run_details = {
            "job_id": job_id,
            "job_name": job_name,
            "run_page_url": run_page_url,
            "start_time": start_time,
            "updated_by": updated_by,
            "end_time": end_time,
            "status": status,
            "error_desc": error_desc,
            "number_of_tasks": number_of_tasks,
        }

        audit_logger = AuditLogger(self.tenant, self.product, self.layer, "insert")

        # act
        audit_logger.insert_job_audit_table(run_details=run_details)

        job_audit = (
            self.spark.read.table(self.job_audit_table_name)
            .filter(f"job_name = '{job_name}'")
            .collect()
        )

        self.assertEqual(
            1, len(job_audit), msg="Too many or no records for tested job audit record."
        )
        job_audit = job_audit[0]

        self.assertEqual(
            job_id,
            job_audit.job_id,
            msg=f"Wrong job_id - expected: {job_id}, but got: {job_audit.job_id}.",
        )

        self.assertEqual(
            end_time,
            job_audit.job_end_time,
            msg=f"Wrong end_time - expected: {end_time}, but got: {job_audit.job_end_time}.",
        )
        self.assertEqual(
            status,
            job_audit.job_status,
            msg=f"Wrong status - expected: {status}, but got: {job_audit.job_status}.",
        )
        self.assertEqual(
            error_desc,
            job_audit.job_error_desc,
            msg=f"Wrong job_error_desc - expected: {error_desc}, but got: {job_audit.job_error_desc}.",
        )
        self.assertEqual(
            updated_by,
            job_audit.updated_by,
            msg=f"Wrong updated_by - expected: {updated_by}, but got: {job_audit.updated_by}.",
        )
        self.assertEqual(
            job_name,
            job_audit.job_name,
            msg=f"Wrong job_name - expected: {job_name}, but got: {job_audit.job_name}.",
        )
        self.assertEqual(
            run_page_url,
            job_audit.job_run_page_url,
            msg=f"Wrong job_run_page_url - expected: {run_page_url}, but got: {job_audit.job_run_page_url}.",
        )
        self.assertEqual(
            start_time,
            job_audit.job_start_time,
            msg=f"Wrong job_start_time - expected: {start_time}, but got: {job_audit.job_start_time}.",
        )
        self.assertEqual(
            number_of_tasks,
            job_audit.number_of_tasks,
            msg=f"Wrong number_of_tasks - expected: {number_of_tasks}, but got: {job_audit.number_of_tasks}.",
        )

        self.assertGreaterEqual(
            job_audit.updated_at,
            current_datetime,
            msg=f"Wrong updated_at - expected time to be greater than: {str(current_datetime)}, but got: {job_audit.updated_at}.",
        )

    def test_insert_task_audit_table(self):
        # arrange
        job_id = 10009
        # run_id = 1009
        # job_run_id = 100098
        job_name = "the job name for task"
        task_name = "the task run name"
        run_page_url = "https://taskrunpageurlinsert.com"
        start_time = datetime.fromisoformat("2023-12-04 00:05:23.283")
        updated_by = "inttest-unity-app"
        end_time = datetime.fromisoformat("2023-12-07 09:28:42.912345")
        status = "TERMINATED"
        error_desc = "some task error with desc"

        current_datetime = datetime.now()

        run_details = {
            "job_name": job_name,
            "job_id": job_id,
            # "job_run_id": job_run_id,
            # "run_id": run_id,
            "task_name": task_name,
            "run_page_url": run_page_url,
            "start_time": start_time,
            "updated_by": updated_by,
            "end_time": end_time,
            "status": status,
            "error_desc": error_desc,
        }

        audit_logger = AuditLogger(self.tenant, self.product, self.layer, "insert")

        # act
        audit_logger.insert_task_audit_table([run_details])

        task_audit = (
            self.spark.read.table(self.task_audit_table_name)
            .filter(f"task_name = '{task_name}'")
            .collect()
        )

        self.assertEqual(
            1,
            len(task_audit),
            msg="Too many or no records for tested task audit record.",
        )
        task_audit = task_audit[0]

        self.assertEqual(
            job_id,
            task_audit.job_id,
            msg=f"Wrong job_id - expected: {job_id}, but got: {task_audit.job_id}.",
        )
        self.assertEqual(
            end_time,
            task_audit.task_end_time,
            msg=f"Wrong task_end_time - expected: {end_time}, but got: {task_audit.task_end_time}.",
        )
        self.assertEqual(
            status,
            task_audit.task_status,
            msg=f"Wrong task_status - expected: {status}, but got: {task_audit.task_status}.",
        )
        self.assertEqual(
            error_desc,
            task_audit.task_error_desc,
            msg=f"Wrong task_error_desc - expected: {error_desc}, but got: {task_audit.task_error_desc}.",
        )
        self.assertEqual(
            updated_by,
            task_audit.updated_by,
            msg=f"Wrong updated_by - expected: {updated_by}, but got: {task_audit.updated_by}.",
        )
        self.assertEqual(
            job_name,
            task_audit.job_name,
            msg=f"Wrong job_name - expected: {job_name}, but got: {task_audit.job_name}.",
        )
        self.assertEqual(
            task_name,
            task_audit.task_name,
            msg=f"Wrong task_name - expected: {task_name}, but got: {task_audit.task_name}.",
        )
        self.assertEqual(
            run_page_url,
            task_audit.task_run_page_url,
            msg=f"Wrong task_run_page_url - expected: {run_page_url}, but got: {task_audit.task_run_page_url}.",
        )
        self.assertEqual(
            start_time,
            task_audit.task_start_time,
            msg=f"Wrong task_start_time - expected: {start_time}, but got: {task_audit.task_start_time}.",
        )
        self.assertGreaterEqual(
            task_audit.updated_at,
            current_datetime,
            msg=f"Wrong updated_at - expected time to be greater than: {str(current_datetime)}, but got: {task_audit.updated_at}.",
        )

    def test_insert_job_audit_table_recovery(self):
        # arrange
        updated_by = "manual"
        expected_status = "RECOVERY_STATUS"
        error_desc = "some task error with desc"

        current_datetime = datetime.now()

        run_details = {
            # "job_name": "999",
            "job_id": 999,
            # "run_id": 999,
            "job_name": f"zzz_{current_datetime}",  # latest name
            "run_page_url": "",
            "updated_by": updated_by,
            "status": expected_status,
            "error_desc": error_desc,
            "number_of_tasks": 999,
            "start_time": current_datetime,
            "end_time": current_datetime,
        }
        audit_logger = AuditLogger(self.tenant, self.product, self.layer, "insert")

        # act
        audit_logger.insert_job_audit_table(run_details=run_details)

        # assert
        status = audit_logger.read_job_audit_table()
        self.assertEqual(
            expected_status,
            status,
            msg=f"Recovered job has status: {status}, but it was expected: {expected_status}.",
        )

    @classmethod
    def tearDownClass(cls):
        print("Testing TestAuditLogger - tearDownClass...")

        cls.spark.sql(f"DROP DATABASE {cls.tenant} CASCADE")
        cls.dbutils.fs.rm(cls.mount_location, True)

        print("Testing TestAuditLogger - tearDownClass... DONE")
