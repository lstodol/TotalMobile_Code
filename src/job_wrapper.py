# Databricks notebook source
import os
import json
import logging

from modules.unity_logging import LoggerConfiguration
from modules.configuration import GlobalConfiguration
from modules.configuration_utils import ConfigurationUtils
from modules.job_manager import JobManager

tenant_name = "shared"

logger = logging.getLogger(__name__)
LoggerConfiguration(tenant_name).configure(logger)

# COMMAND ----------

dbutils.widgets.text("bronze_main_script_file", "load_bronze_table")
dbutils.widgets.text("layer", "bronze")
bronze_main_script_file = dbutils.widgets.get("bronze_main_script_file")
layer = dbutils.widgets.get("layer")

# COMMAND ----------

try:
    # getting abs path for main script file
    logger.info("Checking current directory path...")

    context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
    current_path = context['extraContext']['notebook_path'].rsplit('/', 1)[0]
    bronze_script_path = f"{current_path}/{bronze_main_script_file}"
    
    logger.info(f"Bronze main script path is: {bronze_script_path}")
    
    logger.info("Reading tenant config file from env_config folder...")

    tenant_dict = ConfigurationUtils.read_config(
        GlobalConfiguration.ENV_TENANT_CONFIG_PATH
    )
    logger.info(f"Tenant config read completed...")

    logger.info("Reading product config file from env_config folder...")

    product_dict = ConfigurationUtils.read_config(
        GlobalConfiguration.ENV_PRODUCT_CONFIG_PATH
    )

    logger.info(f"Product config read completed...")

    logger.info("Reading global config file to find out cluster name...")
    global_config = ConfigurationUtils.read_config(
        GlobalConfiguration.GLOBAL_JOBS_CONFIG_PATH
    )
    logger.info("Global config file read completed...")
    
    logger.info("Getting cluster name...")
    cluster_name = global_config["ClusterName"]
    logger.info(f"Found cluster name: {cluster_name}")

    ###create job###
    # loop through tenant and product config dict to create job per tenant and product
    # create an entry in job and task audit table
    logger.info(f"Starting Job creation process...")
    job_manager = JobManager(
        tenant_dict,
        layer,
        cluster_name,
        product_dict,
        bronze_script_path,
    )
    job_manager.create_and_run_job()
    logger.info(f"Job creation process completed.")

except Exception as e:
    logger.error("An exception occurred during job wrapper notebook execution " + str(e))
    raise e
