import logging
from datetime import datetime
from pyspark.sql import SparkSession
from modules.configuration import GlobalConfiguration
from modules.configuration_utils import ConfigurationUtils
from modules.databricks_utils import DatabricksJobManager, AuditLogger
from modules.exceptions import ExceptionCatcher

logger = logging.getLogger(__name__)


class JobManager:
    def __init__(self, tenant_dict, layer, cluster_name = None, product_dict = None, script_path = None):
        self.cluster_name = cluster_name
        self.tenant_dict = tenant_dict
        self.product_dict = product_dict
        self.layer = layer
        self.script_path = script_path
        self.databricks_job_manager = DatabricksJobManager()
        self.cluster_id = self.databricks_job_manager.get_cluster_id(self.cluster_name) if cluster_name != None else None
        self.spark = SparkSession.getActiveSession()
        self.exception_catcher = ExceptionCatcher(context={"class": "JobManager"})

    def task_creation(self, tenant, product, table_list, direction):
        tasks_list = []
        dependent_task = ""

        for table_config in table_list:
            logger.info(f"Creating task using table: {table_config}.")

            schema_name = table_config.split("|")[0].lower()
            table_name = table_config.split("|")[1].lower()
            table_type = table_config.split("|")[2]
            primary_key = table_config.split("|")[3]
            incremental_column = table_config.split("|")[4]
            dependent_task = dependent_task

            logger.info(
                f"""and these parameters: cluster_id: {self.cluster_id}, layer: {self.layer}, tenant: {tenant}, product: {product}, 
                schema_name: {schema_name}, table_name: {table_name}, table_type: {table_type}, primary_key: {primary_key}, incremental_column: {incremental_column}, dependent_task: {dependent_task}, script_path: {self.script_path}"""
            )
            # calling task module to create task
            task = self.databricks_job_manager.create_task(
                self.cluster_id,
                self.layer,
                tenant,
                product,
                schema_name,
                table_name,
                table_type,
                primary_key,
                incremental_column,
                dependent_task,
                self.script_path,
            )
            dependent_task = task.task_key if direction == "series" else ""

            if task:
                logger.info(f"Task creation completed for table: {table_name}.")
                tasks_list.append(task)
            else:
                logger.warn(f"Issue in task creation for table: {table_name}.")

        return tasks_list

    def get_tasks_subset_list(self, tenant, product, tasks_streams, tasks_in_series, start_pos, direction):

        tasks_subset_list = []
        for i in range(tasks_streams):
            end_pos = start_pos + tasks_in_series
            table_list = list(
                (self.product_dict[product])[start_pos:end_pos]
            )
            tasks_list = self.task_creation(
                tenant, 
                product, 
                table_list, 
                direction
            )
            tasks_subset_list.extend(tasks_list)
            start_pos = start_pos + tasks_in_series

        return tasks_subset_list, start_pos


    def get_tasks_full_list(self, tenant, product, total_tasks, parallel_streams, nearest_multiple_parallel_tasks, max_tasks_streams, remaining_tasks_streams):

        start_pos = 0
        tasks_full_list = []

        if max_tasks_streams != total_tasks:
            tasks_in_series = int(
                nearest_multiple_parallel_tasks / parallel_streams
            )

            max_series_tasks, start_pos = self.get_tasks_subset_list(tenant, 
                                                                     product, 
                                                                     max_tasks_streams, 
                                                                     tasks_in_series+1, 
                                                                     start_pos, 
                                                                     "series")
            tasks_full_list.extend(max_series_tasks)
            
            remaining_series_tasks, start_pos = self.get_tasks_subset_list(tenant, 
                                                                           product, 
                                                                           remaining_tasks_streams, 
                                                                           tasks_in_series, 
                                                                           start_pos, 
                                                                           "series")
            tasks_full_list.extend(remaining_series_tasks)

        else:
            tasks_list = self.task_creation(
                tenant, 
                product, 
                self.product_dict[product], 
                "parallel"
            )
            tasks_full_list = tasks_list
        
        return tasks_full_list
    

    def parallelisation(self, tenant, product):

        ###tasks parallelisation logic###
        # get number of parallel streams configured for a job

        logger.info(
        "Reading tenant or global config file to find out number of parallel streams in a job..."
        )
        global_config = GlobalConfiguration.get_job_config(tenant_name=tenant, 
                                                           product_name=product)
        parallel_streams = global_config["TasksParallelStreams"]
        logger.info("Global config file read completed...")
        logger.info(f"Number of parallel streams: {parallel_streams}.")

        if parallel_streams <= 0:
            # default parallel streams
            parallel_streams = 1
            logger.info(f"Taking default value for number of parallel streams as it is configured as 0 in config: {parallel_streams}.")
                            
        total_tasks = len(self.product_dict[product])
        logger.info(f"Total Tasks - {total_tasks}")
        nearest_multiple_parallel_tasks = (
            int(total_tasks / parallel_streams) * parallel_streams
        )
        logger.info(
            f"Nearest multiple of parallel tasks: {nearest_multiple_parallel_tasks}."
        )
        max_tasks_streams = (
            total_tasks - nearest_multiple_parallel_tasks
        )
        logger.info(f"Streams with max tasks: {max_tasks_streams}.")
        remaining_tasks_streams = parallel_streams - max_tasks_streams
        logger.info(
            f"Remaining task streams: {remaining_tasks_streams}."
        )

        return total_tasks, parallel_streams, nearest_multiple_parallel_tasks, max_tasks_streams, remaining_tasks_streams


    def create_and_run_job(self):
        for tenant in self.tenant_dict.keys():
            logger.info(f"Tasks and job creation started for tenant: {tenant}.")
            self.exception_catcher.set_context(tenant=tenant)

            for product in self.tenant_dict[tenant]:
                self.exception_catcher.set_context(product=product)

                try:
                    logger.info(f"Product name is: {product}.")

                    audit_logger = AuditLogger(tenant, product, self.layer, "insert")

                    logger.info(f"Checking job status of last run...")
                    prev_job_status = audit_logger.read_job_audit_table()

                    logger.info(f"Previous job status is: {prev_job_status}.")

                    # Active status - QUEUED, PENDING, RUNNING, BLOCKED or TERMINATING
                    # Completed status - SUCCESS, FAILED, CANCELED, MAXIMUM_CONCURRENT_RUNS_REACHED, SUCCESS_WITH_FAILURES, EXCLUDED, TIMEDOUT, UPSTREAM_CANCELED, UPSTREAM_FAILED
                    if prev_job_status == "SUCCESS" or prev_job_status == "FIRST_RUN":
                        ###create task###
                        logger.info(
                            f"Starting task creation... product: {product}, dict: {self.product_dict}."
                        )

                        ###get parallelisation factor ###
                        total_tasks, parallel_streams, nearest_multiple_parallel_tasks, max_tasks_streams, remaining_tasks_streams = self.parallelisation(tenant, 
                                                                                                                                                          product)
                        
                        ###create tasks###
                        tasks_full_list = self.get_tasks_full_list(tenant, 
                                                                   product, 
                                                                   total_tasks, 
                                                                   parallel_streams, 
                                                                   nearest_multiple_parallel_tasks, 
                                                                   max_tasks_streams, 
                                                                   remaining_tasks_streams)
                        logger.info(
                            f"Task creation completed for tenant: {tenant} and product: {product}."
                            )
                        
                        ###create job###
                        dts = datetime.now()
                        logger.info(
                            f"Starting job creation for tenant: {tenant} and product: {product}."
                        )
                        job_name = f"{self.layer}_{tenant}_{product}_{dts}"
                        logger.info(f"Job name is: {job_name}.")
                        job = DatabricksJobManager().create_job(
                            job_name, tasks_full_list, self.layer, tenant, product
                        )
                        job_id = job.job_id

                        ###insert audit entry###
                        logger.info(f"Starting job creation auditing process...")

                        audit_logger.job_create_update(job_id)
                        logger.info(f"Finishing job creation auditing process...")

                        logger.info(
                            f"Completed job creation for tenant: {tenant} and product: {product}."
                        )
                        
                        ###run job###
                        # run job using job id
                        logger.info(f"Starting job running process...")
                        job_run_id = DatabricksJobManager().run_job(job_id)
                        logger.info(f"Finishing job running process...")
                        logger.info(f"Job run id: {job_run_id}.")

                        ###update audit entry###
                        audit_logger = AuditLogger(tenant, product, self.layer, "run_id_update")

                        logger.info(f"Starting job auditing process...")

                        audit_logger.job_run_update(job_run_id)
                        logger.info(f"Finishing job auditing process.")

                        logger.info(
                            f"Completed job execution for tenant: {tenant} and product: {product}."
                        )

                    else:
                        logger.warn(
                            f"Task and job creation halted because previous job run is in {prev_job_status} state."
                        )
                except Exception as e:
                    self.exception_catcher.catch(e)

        self.exception_catcher.throw_if_any()

    def monitor_job(self):

        # loop through tenants
        logger.info("Looping through the tenants...")
        for tenant in self.tenant_dict.keys():
            logger.info(f"Monitoring for tenant: {tenant}.")
            self.exception_catcher.set_context(tenant_name=tenant)

            for product in self.tenant_dict[tenant]:
                try:
                    logger.info(f"...and product: {product}.")
                    self.exception_catcher.set_context(product=product)
                    audit_logger = AuditLogger(tenant, product, self.layer, "update")
                    job_run_id_df = audit_logger.get_active_jobs()

                    if job_run_id_df.count() == 0:
                        logger.info("No active jobs to track...")
                    else:
                        for run_id in job_run_id_df.collect():
                            job_run_id = run_id["job_run_id"]
                            logger.info(f"Updating status for the active job run id: {job_run_id}.")

                            audit_logger.job_run_update(job_run_id)

                except Exception as e:
                    self.exception_catcher.catch(e)
        
        self.exception_catcher.throw_if_any()