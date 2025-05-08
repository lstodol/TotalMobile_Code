# Databricks notebook source
import logging

from pyspark.sql import SparkSession
from modules.unity_logging import LoggerConfiguration
from modules.configuration import GlobalConfiguration
from modules.configuration_utils import ConfigurationUtils
from modules.job_manager import JobManager
from modules.exceptions import ExceptionCatcher

# COMMAND ----------

tenant_name = "shared"

logger = logging.getLogger(__name__)
LoggerConfiguration(tenant_name).configure(logger)

# COMMAND ----------

dbutils.widgets.text("layer", "bronze")
layer = dbutils.widgets.get("layer")

dbutils.widgets.text("env_tenant_config_path", GlobalConfiguration.ENV_TENANT_CONFIG_PATH)
env_tenant_config_path = dbutils.widgets.get("env_tenant_config_path")

# COMMAND ----------

try:

    # read tenant config
    logger.info("Reading tenant config file from env_config folder...")

    tenant_dict = ConfigurationUtils.read_config(
        env_tenant_config_path
    )
    
    logger.info(f"Tenant config read completed...")

    # loop through tenant config dict to monitor job per tenant and product
    # update job and task audit table
    logger.info(f"###Starting job monitoring process###")
    job_monitor = JobManager(
        tenant_dict,
        layer
    )
    job_monitor.monitor_job()
    logger.info(f"###Job monitoring process completed###")

except Exception as e:
    logger.error("An exception occurred during job monitoring notebook execution " + str(e))
    raise e

# COMMAND ----------


