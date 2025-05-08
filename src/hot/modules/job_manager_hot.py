import logging
from pyspark.sql import SparkSession

from hot.modules.databricks_utils_hot import DatabricksJobManagerHot
from hot.modules.configuration_hot import GlobalConfigurationHot
from modules.exceptions import ExceptionCatcher

logger = logging.getLogger(__name__)

class JobManagerHot:
    def __init__(self, hot_tenant_product_dict, hot_tenant_domain_dict, hot_product_dict, hot_domain_dict, ingestor_script_path, domain_script_path, cluster_name=None):

        self.cluster_name = cluster_name
        self.hot_tenant_product_dict = hot_tenant_product_dict
        self.hot_tenant_domain_dict = hot_tenant_domain_dict
        self.hot_product_dict = hot_product_dict
        self.hot_domain_dict = hot_domain_dict
        self.ingestor_script_path = ingestor_script_path
        self.domain_script_path = domain_script_path
        self.databricks_job_manager_hot = DatabricksJobManagerHot()
        self.cluster_id = self.databricks_job_manager_hot.get_cluster_id(self.cluster_name) if cluster_name is not None else None
        self.spark = SparkSession.getActiveSession()
        self.exception_catcher = ExceptionCatcher(context={"class": "JobManagerHot"})


    def create_ingestion_task(self, tenant, product, event_hub_namespace, entity):
        
        logger.info(f"Creating ingestion task for tenant: {tenant} / product: {product} / event_hub_namespace: {event_hub_namespace} / entity: {entity}.")
        
        task = self.databricks_job_manager_hot.create_ingestion_task(
            self.cluster_id,
            tenant,
            product,
            event_hub_namespace,
            entity,
            self.ingestor_script_path
        )
        
        logger.info(f"Ingestion task creation completed for tenant: {tenant} / product: {product} / event_hub_namespace: {event_hub_namespace} / entity: {entity}.")

        return task 

    def create_ingestion_job(self):

        level = "product"

        for tenant in self.hot_tenant_product_dict.keys():
            logger.info(f"Task and ingestion job creation started for tenant: {tenant}.")
            self.exception_catcher.set_context(tenant=tenant)

            for product in self.hot_tenant_product_dict[tenant]:
                self.exception_catcher.set_context(product=product)
                try:
                    logger.info(
                        f"Starting ingestion job creation for tenant: {tenant} and product: {product}."
                    )
                    ingestion_job_name = f"ingestor_{tenant}_{product}"
                    logger.info(f"Creating ingestion job: {ingestion_job_name}...")
                    event_hub_namespace = GlobalConfigurationHot.get_event_hub_namespace(tenant, product)
                    
                    ingestion_task_list = []
                    for item in self.hot_product_dict[product]:
                        parts = item.split("|")
                        table = parts[0].lower()
                        layer = parts[1].lower()
                       
                        if layer == "bronze":               
                            ingestion_task_list.append(self.create_ingestion_task(tenant, product, event_hub_namespace, table))
                        
                    logger.info(
                        f"Calling create_ingestion_job {ingestion_job_name}..."
                    )
                    self.databricks_job_manager_hot.create_ingestion_job(
                        ingestion_job_name, ingestion_task_list, tenant, product, level, load_type="stream"
                    )
                    logger.info(
                        f"Completed ingestion job creation: {ingestion_job_name}."
                    )
                except Exception as e:
                    self.exception_catcher.catch(e)

        self.exception_catcher.throw_if_any()

    def create_domain_task(self, tenant, domain, task_lists, load_type):
        logger.info("Fetching silver and gold tables from domain dictionary...")

        main_script = self.domain_script_path + "/" + f"{domain}" + "/main"

        domain_task_list = []

        for item in task_lists:
            parts = item.split("|")
            table = parts[0].lower()
            primary_keys = parts[1].lower()
            layer = parts[2].lower()
            load_type = parts[3].lower()

            logger.info(f"Creating domain task for tenant: {tenant} / domain: {domain} / table: {table} / primary_keys: {primary_keys} / data layer: {layer}.")

            task = self.databricks_job_manager_hot.create_domain_task(
                self.cluster_id,
                tenant,
                domain,
                table,
                primary_keys,
                layer,
                load_type,
                main_script
            )
            if task:
                logger.info(f"Domain task creation completed for tenant: {tenant} / domain: {domain} / table: {table} / primary_keys: {primary_keys} / data layer: {layer}.")
                domain_task_list.append(task)
            else:
                logger.warn(f"Issue in pipeline task creation for tenant: {tenant} / domain: {domain} / table: {table} / primary_keys: {primary_keys} / data layer: {layer}.")

        return domain_task_list
    
    def create_domain_job(self):
        
        level = "domain"

        for tenant in self.hot_tenant_domain_dict.keys():
            logger.info(f"Task and domain job creation started for tenant: {tenant}.")
            self.exception_catcher.set_context(tenant=tenant)

            for domain in self.hot_tenant_domain_dict[tenant]:
                self.exception_catcher.set_context(domain=domain)
                try:
                    # Create stream domain job
                    domain_stream_job_name = f"domain_stream_{tenant}_{domain}"
                    logger.info(f"Creating stream domain job: {domain_stream_job_name}...")

                    stream_task_list = [task for task in self.hot_domain_dict[domain] if "|stream" in task]
                    domain_stream_task_list = self.create_domain_task(tenant, domain, stream_task_list, load_type='stream')
                    if domain_stream_task_list:
                        self.databricks_job_manager_hot.create_or_update_job(
                            domain_stream_job_name, domain_stream_task_list, tenant, domain, level, load_type='stream'
                        )
                        logger.info(f"Completed stream domain job creation: {domain_stream_job_name}.")
                    
                    # Create batch domain job
                    domain_batch_job_name = f"domain_batch_{tenant}_{domain}"
                    logger.info(f"Creating batch domain job: {domain_batch_job_name}...")

                    batch_task_list = [task for task in self.hot_domain_dict[domain] if "|batch" in task]
                    domain_batch_task_list = self.create_domain_task(tenant, domain, batch_task_list, load_type='batch')
                    if domain_batch_task_list:
                        self.databricks_job_manager_hot.create_or_update_job(
                            domain_batch_job_name, domain_batch_task_list, tenant, domain, level, load_type='batch'
                        )
                        logger.info(f"Completed batch domain job creation: {domain_batch_job_name}.")

                except Exception as e:
                    self.exception_catcher.catch(e)

        self.exception_catcher.throw_if_any()
