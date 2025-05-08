# Databricks notebook source
import os
import json
import logging

from modules.unity_logging import LoggerConfiguration
from modules.configuration_utils import ConfigurationUtils
from hot.modules.configuration_hot import GlobalConfigurationHot
from hot.modules.job_manager_hot import JobManagerHot

tenant_name = "shared"

logger = logging.getLogger(__name__)
LoggerConfiguration(tenant_name).configure(logger)

# COMMAND ----------

dbutils.widgets.text("main_ingestor_notebook", "hot/ingestor/main")
main_ingestor_notebook = dbutils.widgets.get("main_ingestor_notebook")

dbutils.widgets.text("main_domain_notebook_path", "hot/domain")
main_domain_notebook_path = dbutils.widgets.get("main_domain_notebook_path")

# COMMAND ----------

try:
    logger.info("Retrieving main ingestor & domain absolute file paths...")

    context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
    current_path = context['extraContext']['notebook_path'].rsplit('/', 1)[0]

    ingestor_script_path = f"{current_path}/{main_ingestor_notebook}"
    domain_script_path = f"{current_path}/{main_domain_notebook_path}"

    logger.info("Reading tenant product config file from env_config folder...")

    hot_tenant_product_dict = ConfigurationUtils.read_config(
        GlobalConfigurationHot.ENV_TENANT_PRODUCT_CONFIG_PATH_HOT
    )
    logger.info(f"Tenant product config read completed.")

    logger.info("Reading tenant domain config file from env_config folder...")

    hot_tenant_domain_dict = ConfigurationUtils.read_config(
        GlobalConfigurationHot.ENV_TENANT_DOMAIN_CONFIG_PATH_HOT
    )
    logger.info(f"Tenant domain config read completed.")

    logger.info("Reading product config file from env_config folder...")

    hot_product_dict = ConfigurationUtils.read_config(
        GlobalConfigurationHot.ENV_PRODUCT_CONFIG_PATH_HOT
    )
    logger.info(f"Product config read completed.")

    logger.info("Reading domain config file from env_config folder...")

    hot_domain_dict = ConfigurationUtils.read_config(
        GlobalConfigurationHot.ENV_DOMAIN_CONFIG_PATH_HOT
    )
    logger.info(f"Domain config read completed.")

    global_config = ConfigurationUtils.read_config(
        GlobalConfigurationHot.GLOBAL_JOBS_CONFIG_PATH
    )
    cluster_name = global_config["ClusterName"]

    logger.info(f"Global config read completed. Found cluster name: {cluster_name}.")

    logger.info(f"Starting job creation process...")
    hot_job_manager = JobManagerHot(
        hot_tenant_product_dict, hot_tenant_domain_dict, hot_product_dict, hot_domain_dict, ingestor_script_path, domain_script_path, cluster_name
    )

    hot_job_manager.create_ingestion_job()
    hot_job_manager.create_domain_job()

    logger.info(f"Job creation process completed.")

except Exception as e:
    logger.error(
        "An exception occurred during hot job wrapper execution " + str(e)
    )
    raise e
