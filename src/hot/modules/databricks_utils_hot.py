import logging
from databricks.sdk.service import jobs
from databricks.sdk.service.jobs import PauseStatus
from databricks.sdk import WorkspaceClient
from modules.exceptions import ClusterNotFoundException, ExceptionCatcher
from hot.modules.configuration_hot import GlobalConfigurationHot

logger = logging.getLogger(__name__)

class DatabricksJobManagerHot:
    def __init__(self):
        self.w = DatabricksJobManagerHot.databricks_login()
        self.exception_catcher = ExceptionCatcher()

    def databricks_login() -> WorkspaceClient:
        try:
            logger.info("Creating workspace client object...")
            w = WorkspaceClient()
            return w
        except Exception as e:
            logger.error(f"Encountered error: {e} while creating the workspace object.")
            raise e

    def get_cluster_id(self, cluster_name: str) -> str:
        logger.info("Listing clusters in this workspace...")
        cluster_list = self.w.clusters.list()

        logger.info("Finding required cluster in this workspace...")
        for cluster in cluster_list:
            if cluster.cluster_name == cluster_name:
                cluster_id = cluster.cluster_id
                logger.info(
                    f"Found the required cluster: {cluster_name} with id: {cluster_id}."
                )
                return cluster_id

        raise ClusterNotFoundException(
            f"Cluster: {cluster_name} not found. Please check the cluster name provided."
        )

    def get_job_id(self, job_name: str) -> str:
        jobs_list = self.w.jobs.list()
        for job in jobs_list:
            if job.settings.name == job_name:
                return job.job_id
        return None

    def update_job(self, job_id: str, job_name: str, task_list: list, tenant: str, product_or_domain: str, level: str, load_type: str) -> object:
        try:
            if level == "product":
                product = product_or_domain
                job_config = GlobalConfigurationHot.get_job_config(
                    tenant_name=tenant, product_name=product
                    )
            else:
                domain = product_or_domain
                job_config = GlobalConfigurationHot.get_job_config(
                    tenant_name=tenant, domain_name=domain
                    )

            if level == "product":
                tags={tenant: "", product: "", load_type: ""}
            else:
                tags={tenant: "", domain: "", load_type: ""}

            self.w.jobs.update(
                job_id=job_id,
                new_settings=jobs.JobSettings(
                    name=job_name,
                    tasks=task_list,
                    tags=tags,
                    email_notifications=jobs.JobEmailNotifications(
                        no_alert_for_skipped_runs=True,
                        on_failure=job_config["NotificationRecipients"],
                        on_start=None,
                        on_success=None,
                    ),
                    notification_settings=jobs.JobNotificationSettings(
                        no_alert_for_canceled_runs=True, no_alert_for_skipped_runs=True
                    ),
                )
            )
        except Exception as e:
            logger.error(f"Encountered error: {e} while updating job.")
            raise e

    def create_or_update_job(self, job_name: str, task_list: list, tenant: str, product_or_domain: str,  level: str, load_type: str) -> object:
        job_id = self.get_job_id(job_name)
        if job_id:
            logger.info(f"Updating existing job: {job_name}")
            self.update_job(job_id, job_name, task_list, tenant, product_or_domain, level, load_type)
        else:
            logger.info(f"Creating new job: {job_name}")
            self.create_job(job_name, task_list, tenant, product_or_domain, level, load_type)

    def create_job(self, job_name: str, task_list: list, tenant: str, product_or_domain: str,  level: str, load_type: str) -> object:
        try:
            
            if level == "product":
                product = product_or_domain
                job_config = GlobalConfigurationHot.get_job_config(
                    tenant_name=tenant, product_name=product
                    )
            else:
                domain = product_or_domain
                job_config = GlobalConfigurationHot.get_job_config(
                    tenant_name=tenant, domain_name=domain
                    )

            if load_type == "batch":
                health = jobs.JobsHealthRules(
                    rules=[
                        jobs.JobsHealthRule(
                            metric=jobs.JobsHealthMetric.RUN_DURATION_SECONDS,
                            op=jobs.JobsHealthOperator.GREATER_THAN,
                            value=job_config["JobDurationThreshold"],
                        )
                    ]
                )
                schedule = jobs.CronSchedule(
                    quartz_cron_expression=job_config["JobCronSchedule"],
                    timezone_id="UTC",
                    pause_status=PauseStatus.PAUSED,
                )
                timeout_seconds = job_config["JobTimeoutThreshold"]

            else:
                health = None
                schedule = None
                timeout_seconds = None

            if level == "product":
                tags={tenant: "", product: "", load_type: ""}
            else:
                tags={tenant: "", domain: "", load_type: ""}

            job = self.w.jobs.create(
                name=f"{job_name}",
                tasks=task_list,
                tags=tags,
                timeout_seconds=timeout_seconds,
                health=health,
                email_notifications=jobs.JobEmailNotifications(
                    no_alert_for_skipped_runs=True,
                    on_failure=job_config["NotificationRecipients"],
                    on_start=None,
                    on_success=None,
                ),
                notification_settings=jobs.JobNotificationSettings(
                    no_alert_for_canceled_runs=True, no_alert_for_skipped_runs=True
                ),
                schedule=schedule,
            )
            return job
        except Exception as e:
            logger.fatal(f"Encountered error: {e} while creating a job.")
            raise e

    def create_ingestion_task(
        self,
        cluster_id: str,
        tenant: str,
        product: str,
        event_hub_namespace: str,
        entity: str,
        ingestor_script_path: str,
    ) -> object:
        try:
            logger.debug(f"Creating ingestion task: {tenant}_{product}_{entity}")
            task = jobs.Task(
                description=f"{tenant}_{product}_{entity}",
                existing_cluster_id=f"{cluster_id}",
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{ingestor_script_path}",
                    base_parameters={
                        "tenant_name": f"{tenant}",
                        "product_name": f"{product}",
                        "event_hub_namespace": f"{event_hub_namespace}",
                        "entity_name": f"{entity}",
                    },
                ),
                max_retries=-1,
                min_retry_interval_millis=10000,
                task_key=f"{tenant}_{product}_{entity}",
            )
            return task
        except Exception as e:
            logger.error(f"Encountered error: {e} while creating a task.")
            raise e

    def create_domain_task(
        self,
        cluster_id: str,
        tenant: str,
        domain: str,
        table: str,
        primary_keys: str,
        layer: str,
        load_type: str,
        domain_script_path: str,
    ) -> object:
        try:
            if load_type == "batch":
                task = jobs.Task(
                    description=f"{layer}_{load_type}_{table}",
                    existing_cluster_id=f"{cluster_id}",
                    notebook_task=jobs.NotebookTask(
                        notebook_path=f"{domain_script_path}",
                        base_parameters={
                            "tenant_name": f"{tenant}",
                            "domain_name": f"{domain}",
                            "table_name": f"{table}",
                            "primary_keys": f"{primary_keys}",
                            "data_layer": f"{layer}",
                        },
                    ),
                    max_retries=1,
                    min_retry_interval_millis=0,
                    task_key=f"{layer}_{load_type}_{table}",
                )
            else:
                task = jobs.Task(
                    description=f"{layer}_{load_type}_{table}",
                    existing_cluster_id=f"{cluster_id}",
                    notebook_task=jobs.NotebookTask(
                        notebook_path=f"{domain_script_path}",
                        base_parameters={
                            "tenant_name": f"{tenant}",
                            "domain_name": f"{domain}",
                            "table_name": f"{table}",
                            "primary_keys": f"{primary_keys}",
                            "data_layer": f"{layer}",
                        },
                    ),
                    max_retries=-1,
                    min_retry_interval_millis=10000,
                    task_key=f"{layer}_{load_type}_{table}",
                )
            return task
        except Exception as e:
            logger.error(f"Encountered error: {e} while creating a task.")
            raise e
        
    def create_ingestion_job(
        self, job_name: str, task_list: list, tenant: str, product: str, level: str, load_type: str
    ) -> object:
        self.create_or_update_job(job_name, task_list, tenant, product, level, load_type)

    def create_domain_job(
        self, job_name: str, task_list: list, tenant: str, domain: str, level: str, load_type: str
    ) -> object:
        self.create_or_update_job(job_name, task_list, tenant, domain, level, load_type)
