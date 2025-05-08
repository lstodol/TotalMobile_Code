import logging
from datetime import datetime
from pyspark.sql import SparkSession, Row, DataFrame
from databricks.sdk.service import jobs
from databricks.sdk import WorkspaceClient
from modules.exceptions import ClusterNotFoundException, ExceptionCatcher
from delta.tables import DeltaTable
from modules.configuration import GlobalConfiguration

logger = logging.getLogger(__name__)


class DatabricksJobManager:
    def __init__(self):
        self.w = DatabricksJobManager.databricks_login()
        self.exception_catcher = ExceptionCatcher()

    def databricks_login() -> WorkspaceClient:
        # create workspace client object
        try:
            logger.info("Creating workspace client object...")
            w = WorkspaceClient()
            return w

        except Exception as e:
            logger.error(f"Encountered error: {e} while creating the workspace object.")
            raise e

    def get_cluster_id(self, cluster_name: str) -> str:
        # cluster list from this workspace
        logger.info("Listing clusters in this workspace...")
        cluster_list = self.w.clusters.list()

        # get cluster info
        logger.info("Finding required cluster in this workspace...")
        for cluster in cluster_list:
            if cluster.cluster_name == cluster_name:
                logger.info("Found the required cluster...")
                cluster_id = cluster.cluster_id

                logger.info(f"Cluster Name is: {cluster_name}.")
                logger.info(f"Cluster Id is: {cluster_id}.")

                return cluster_id

        raise ClusterNotFoundException(
            f"Cluster {cluster_name} not found. Please check the cluster name provided."
        )

    def create_task(
        self,
        cluster_id: str,
        stage: str,
        tenant: str,
        product: str,
        schema_name: str,
        table_name: str,
        table_type: str,
        primary_key: str,
        incremental_column: str,
        dependent_task: str,
        script_path: str,
    ) -> object:
        try:
            task = jobs.Task(
                description=f"{stage}_{schema_name}_{table_name}",
                existing_cluster_id=f"{cluster_id}",
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{script_path}",
                    base_parameters={
                        "tenant_name": f"{tenant}",
                        "product_name": f"{product}",
                        "schema_name": f"{schema_name}",
                        "table_name": f"{table_name}",
                        "table_type": f"{table_type}",
                        "primary_key": f"{primary_key}",
                        "incremental_column": f"{incremental_column}",
                    },
                ),
                max_retries=1,
                task_key=f"{stage}_{schema_name}_{table_name}",
                depends_on=(
                    [jobs.TaskDependency(task_key=f"{dependent_task}")]
                    if dependent_task != ""
                    else None
                ),
                run_if=jobs.RunIf("ALL_DONE") if dependent_task != "" else None,
                timeout_seconds=0,
            )
            return task
        except Exception as e:
            logger.error(f"Encountered error: {e} while creating a task.")
            raise e

    def create_job(
        self, job_name: str, tasks_list: list, stage: str, tenant: str, product: str
    ) -> object:
        try:

            job_config = GlobalConfiguration.get_job_config(
                tenant_name=tenant, product_name=product
            )

            job = self.w.jobs.create(
                name=f"{job_name}",
                tasks=tasks_list,
                tags={f"{stage}": "", f"{tenant}": "", f"{product}": ""},
                timeout_seconds=job_config["JobTimeoutThreshold"],
                health=jobs.JobsHealthRules(
                    rules=[
                        jobs.JobsHealthRule(
                            metric=jobs.JobsHealthMetric.RUN_DURATION_SECONDS,
                            op=jobs.JobsHealthOperator.GREATER_THAN,
                            value=job_config["JobDurationThreshold"],
                        )
                    ]
                ),
                email_notifications=jobs.JobEmailNotifications(
                    no_alert_for_skipped_runs=True,
                    on_duration_warning_threshold_exceeded=job_config[
                        "NotificationRecipients"
                    ],
                    on_failure=job_config["NotificationRecipients"],
                    on_start=None,
                    on_success=None,
                ),
                notification_settings=jobs.JobNotificationSettings(
                    no_alert_for_canceled_runs=True, no_alert_for_skipped_runs=True
                ),
            )

            return job
        except Exception as e:
            logger.fatal(f"Encountered error: {e} while creating a job.")
            raise e

    def run_job(self, job_id: int) -> int:
        try:
            logger.info(f"Running the job with job id: {job_id}.")
            run_by_id = self.w.jobs.run_now(job_id=job_id)
            logger.info(f"Executed the job with job id: {job_id}.")
            run_id = run_by_id.run_id
            logger.info(f"Run id for the job is: {run_id}.")
            return run_id

        except Exception as e:
            logger.error(f"Encountered error: {e} while running a job.")
            raise e

    @staticmethod
    def convert_unix_to_datetime(unix_time_ms):
        unix_time_s = unix_time_ms / 1000  # Convert from ms to s
        return datetime.fromtimestamp(unix_time_s)  # Convert from s to datetime object

    def cleanup_jobs(self, thresholds: dict):
        current_time = datetime.now()
        logger.debug(f"Current time is: {current_time}")
        logger.debug(f"Thresholds are: {thresholds}.")

        self.exception_catcher.set_context(actvitiy="job_cleanup")

        for j in self.w.jobs.list(expand_tasks=True):
            try:
                self.exception_catcher.set_context(job_name=j.settings.name)
                # no default thresholds, so if not defined it will raise error
                threshold = None
                # find threshold for tenant
                for key in thresholds.keys():
                    if key in j.settings.name:
                        threshold = thresholds[key]

                if threshold is None:
                    continue

                # delete job
                job_datetime = datetime.fromisoformat(j.settings.name[-26:])
                job_delta = current_time - job_datetime

                logger.debug(f"Job delta is : {job_delta} and threshold {threshold}")

                if threshold < job_delta.days:

                    logger.debug(
                        f"Job {j.settings.name} will be deleted because the delta is: {job_delta}"
                    )
                    self.w.jobs.delete(j.job_id)

            except Exception as e:
                self.exception_catcher.catch(e)
                continue

        self.exception_catcher.throw_if_any()


class AuditLogger:
    def __init__(self, tenant, product, layer, action):
        self.w = DatabricksJobManager.databricks_login()
        self.tenant = tenant
        self.product = product
        self.layer = layer
        self.action = action

        self.spark = SparkSession.getActiveSession()

        self.job_audit_table_name = f"{tenant}.{layer}_{product}_job_audit"
        self.task_audit_table_name = f"{tenant}.{layer}_{product}_task_audit"

    def init_db(self, teanant_location):

        location = f"{teanant_location}/{self.layer}"

        self.spark.sql(
            f"""
                        CREATE TABLE IF NOT EXISTS {self.job_audit_table_name} (
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
                        LOCATION '{location}/{self.job_audit_table_name}'
                        TBLPROPERTIES ('delta.autoOptimize.autoCompact'='true', 'delta.autoOptimize.optimizeWrite'='true' )"""
        )

        self.spark.sql(
            f"""
                        CREATE TABLE IF NOT EXISTS {self.task_audit_table_name} (
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
                        LOCATION '{location}/{self.task_audit_table_name}'
                        TBLPROPERTIES ('delta.autoOptimize.autoCompact'='true', 'delta.autoOptimize.optimizeWrite'='true' )"""
        )

    def upd_job_audit(self, run_details):

        current_datetime = datetime.now()

        self.spark.sql(
            f"""update {self.job_audit_table_name}
                set job_start_time = '{run_details['start_time']}'
                , job_end_time = '{run_details['end_time']}'
                , job_status = '{run_details['status']}'
                , job_error_desc = '{run_details['error_desc']}'
                , updated_by = '{run_details['updated_by']}'
                , updated_at = '{current_datetime}' 
                where job_run_id = {run_details['run_id']}"""
        )

    def run_id_upd_job_audit(self, run_details):

        current_datetime = datetime.now()

        self.spark.sql(
            f"""update {self.job_audit_table_name}
                set job_run_id = '{run_details['run_id']}' 
                , job_start_time = '{run_details['start_time']}'
                , job_end_time = '{run_details['end_time']}'
                , job_run_page_url = '{run_details['run_page_url']}'
                , number_of_tasks = '{run_details['number_of_tasks']}'
                , job_status = '{run_details['status']}'
                , job_error_desc = '{run_details['error_desc']}'
                , updated_by = '{run_details['updated_by']}'
                , updated_at = '{current_datetime}' 
                where job_id = {run_details['job_id']}"""
        )

    def upd_task_audit(self, task_audit_update_df):

        location = (
            self.spark.sql(f"desc formatted {self.task_audit_table_name}")
            .filter("col_name=='Location'")
            .collect()[0]
            .data_type
        )
        task_audit_table = DeltaTable.forPath(self.spark, location)

        task_audit_update_df.show()

        (
            task_audit_table.alias("task")
            .merge(
                task_audit_update_df.alias("update"),
                """task.task_run_id = update.task_run_id""",
            )
            .whenMatchedUpdate(
                set={
                    "tenant": "update.tenant",
                    "product": "update.product",
                    "task_start_time": "update.task_start_time",
                    "job_name": "update.job_name",
                    "job_id": "update.job_id",
                    "task_name": "update.task_name",
                    "task_error_desc": "update.task_error_desc",
                    "task_status": "update.task_status",
                    "task_end_time": "update.task_end_time",
                    "task_run_page_url": "update.task_run_page_url",
                    "job_run_id": "update.job_run_id",
                    "task_run_id": "update.task_run_id",
                    "updated_by": "update.updated_by",
                    "updated_at": "update.updated_at",
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    def run_id_upd_task_audit(self, task_audit_update_df):

        location = (
            self.spark.sql(f"desc formatted {self.task_audit_table_name}")
            .filter("col_name=='Location'")
            .collect()[0]
            .data_type
        )
        task_audit_table = DeltaTable.forPath(self.spark, location)

        task_audit_update_df.show()
        print(task_audit_update_df.schema)

        (
            task_audit_table.alias("task")
            .merge(
                task_audit_update_df.alias("update"),
                """task.job_id = update.job_id and task.task_name = update.task_name""",
            )
            .whenMatchedUpdate(
                set={
                    "tenant": "update.tenant",
                    "product": "update.product",
                    "task_start_time": "update.task_start_time",
                    "job_name": "update.job_name",
                    "job_id": "update.job_id",
                    "task_name": "update.task_name",
                    "task_error_desc": "update.task_error_desc",
                    "task_status": "update.task_status",
                    "task_end_time": "update.task_end_time",
                    "task_run_page_url": "update.task_run_page_url",
                    "job_run_id": "update.job_run_id",
                    "task_run_id": "update.task_run_id",
                    "updated_by": "update.updated_by",
                    "updated_at": "update.updated_at",
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    def update_job_audit_table(self, run_details):

        if self.action == "update":
            self.upd_job_audit(run_details)

        elif self.action == "run_id_update":
            self.run_id_upd_job_audit(run_details)

        logger.info(f"Update for job {run_details['run_name']} completed.")

    def update_task_audit_table(self, task_details_list):

        updated_at = datetime.now()
        rows = []
        for run_details in task_details_list:
            rows.append(
                Row(
                    tenant=self.tenant,
                    product=self.product,
                    job_id=run_details["job_id"],
                    job_name=run_details["job_name"],
                    job_run_id=run_details["job_run_id"],
                    task_name=run_details["run_name"],
                    task_run_id=run_details["run_id"],
                    task_start_time=run_details["start_time"],
                    task_end_time=run_details["end_time"],
                    task_run_page_url=run_details["run_page_url"],
                    task_status=run_details["status"],
                    task_error_desc=run_details["error_desc"],
                    updated_by=run_details["updated_by"],
                    updated_at=updated_at,
                )
            )

        task_audit_update_df = self.spark.createDataFrame(rows)

        if self.action == "update":
            self.upd_task_audit(task_audit_update_df)

        elif self.action == "run_id_update":
            self.run_id_upd_task_audit(task_audit_update_df)

        logger.info(
            f"Cumulative update of task details: (len: {len(task_details_list)}) for job completed."
        )

    def insert_job_audit_table(self, run_details):

        df = self.spark.createDataFrame(
            [
                Row(
                    tenant=self.tenant,
                    product=self.product,
                    job_name=run_details["job_name"],
                    job_id=run_details["job_id"],
                    job_start_time=run_details["start_time"],
                    job_end_time=run_details["end_time"],
                    job_run_page_url=run_details["run_page_url"],
                    number_of_tasks=run_details["number_of_tasks"],
                    job_status=run_details["status"],
                    job_error_desc=run_details["error_desc"],
                    updated_by=run_details["updated_by"],
                    updated_at=datetime.now(),
                )
            ]
        )
        (
            df.write.format("delta")
            .option("mergeSchema", "true")
            .mode("append")
            .saveAsTable(self.job_audit_table_name)
        )

        logger.info(f"Insert for job {run_details['job_name']} completed.")

    def insert_task_audit_table(self, task_details_list):

        updated_at = datetime.now()
        rows = []
        for run_details in task_details_list:
            rows.append(
                Row(
                    tenant=self.tenant,
                    product=self.product,
                    job_name=run_details["job_name"],
                    job_id=run_details["job_id"],
                    task_name=run_details["task_name"],
                    task_start_time=run_details["start_time"],
                    task_end_time=run_details["end_time"],
                    task_run_page_url=run_details["run_page_url"],
                    task_status=run_details["status"],
                    task_error_desc=run_details["error_desc"],
                    updated_by=run_details["updated_by"],
                    updated_at=updated_at,
                )
            )

        df = self.spark.createDataFrame(rows)

        (
            df.write.format("delta")
            .option("mergeSchema", "true")
            .mode("append")
            .saveAsTable(self.task_audit_table_name)
        )

        logger.info(
            f"Cumulative insert of task details: (len: {len(task_details_list)}) for job completed."
        )

    def read_job_audit_table(self) -> str:
        logger.info(f"Taking status of previous job run...")
        status = self.spark.sql(
            f"""select job_status from {self.job_audit_table_name} 
                where tenant = '{self.tenant}' and product = '{self.product}' 
                order by job_name desc 
                limit 1"""
        )

        if status.count() > 0:
            job_status = status.collect()[0][0]
            logger.info(f"Previous job status is: {job_status}.")
        else:
            job_status = "FIRST_RUN"
            logger.info(
                f"No entry in audit log so considering this one as first run..."
            )

        return job_status

    def get_active_jobs(self) -> DataFrame:

        logger.info(f"Reading audit table for monitoring: {self.job_audit_table_name}.")

        logger.info("Getting the run id(s) where job is in one of the ACTIVE status...")

        job_run_id_df = self.spark.sql(
            f"""select job_run_id 
                                      from {self.job_audit_table_name}
                                      where job_status IN('QUEUED', 'PENDING', 'RUNNING', 'TERMINATING', 'BLOCKED', 'WAITING_FOR_RETRY')"""
        )

        return job_run_id_df

    def get_run_details(self, run_output):
        run_details = {}

        run_details["job_id"] = run_output.job_id
        run_details["run_id"] = run_output.run_id
        run_details["run_name"] = run_output.run_name
        logger.info(
            f"Collecting all required run details for {run_details['run_name']}..."
        )
        run_details["number_of_tasks"] = len(run_output.tasks)
        run_details["run_page_url"] = run_output.run_page_url
        start_epoch_time = (run_output.start_time) / 1000
        run_details["start_time"] = str(datetime.fromtimestamp(start_epoch_time))
        run_details["updated_by"] = "unity-app"
        state = str(run_output.state.life_cycle_state).split(".")[1]
        logger.info(f"Current state is - {state}")

        if state not in [
            "RUNNING",
            "TERMINATING",
            "PENDING",
            "QUEUED",
            "BLOCKED",
            "WAITING_FOR_RETRY",
        ]:
            end_epoch_time = (run_output.end_time) / 1000
            run_details["end_time"] = str(datetime.fromtimestamp(end_epoch_time))
            run_details["status"] = str(run_output.state.result_state).split(".")[1]
            run_details["error_desc"] = run_output.state.state_message
        else:
            run_details["end_time"] = ""
            run_details["status"] = state
            run_details["error_desc"] = ""

        logger.info(f"Run details are: {run_details}")
        return run_details

    def task_run_update(self, job_run_output, job_name, job_run_id):
        logger.info(f"Looping through all tasks...")

        task_details_list = []

        for i, task in enumerate(job_run_output.tasks):
            task_run_output = self.w.jobs.get_run_output(run_id=task.run_id)
            logger.info(
                "Collected all required task run details for task run id: {task.run_id}."
            )
            run_details = dict(self.get_run_details(task_run_output.metadata))
            run_details["job_name"] = job_name
            run_details["job_run_id"] = job_run_id
            task_details_list.append(run_details)

        self.update_task_audit_table(task_details_list)

    def job_run_update(self, job_run_id):
        job_run_output = self.w.jobs.get_run(run_id=job_run_id)
        logger.info("Converting job run output to job run details...")
        run_details = self.get_run_details(job_run_output)
        job_name = run_details["run_name"]
        job_run_id = run_details["run_id"]
        logger.info("Collected all required job run details...")
        logger.info("Updating job audit table...")
        self.update_job_audit_table(run_details)

        # task audit
        logger.info(f"Starting process for task audit update...")
        self.task_run_update(job_run_output, job_name, job_run_id)

    def get_job_details(self, job_output):
        job_details = {}

        job_details["job_id"] = job_output.job_id
        job_details["job_name"] = job_output.settings.name
        job_details["start_time"] = ""
        job_details["end_time"] = ""
        job_details["run_page_url"] = ""
        job_details["number_of_tasks"] = len(job_output.settings.tasks)
        job_details["status"] = "CREATED"
        job_details["error_desc"] = ""
        job_details["updated_by"] = "unity-app"

        logger.info(f"Job details are: {job_details}")
        return job_details

    def task_create_update(self, job_output):
        logger.info(f"Looping through all tasks...")

        task_details_list = []

        for i, task in enumerate(job_output.settings.tasks):
            task_details = dict(self.get_job_details(job_output))
            task_details["task_name"] = task.task_key
            logger.info("Collected all required task run details...")
            logger.info("Updating task audit table...")
            logger.info(
                f"Running audit update for task: {task_details['task_name']}..."
            )
            task_details_list.append(task_details)

        self.insert_task_audit_table(task_details_list)

    def job_create_update(self, job_id):
        job_output = self.w.jobs.get(job_id=job_id)
        logger.info("Converting job output to job details...")
        job_details = self.get_job_details(job_output)
        logger.info("Collected all required job details...")
        logger.info("Updating job audit table...")
        self.insert_job_audit_table(job_details)

        # task audit
        logger.info(f"Starting process for task audit update...")
        self.task_create_update(job_output)
