# Databricks notebook source
# MAGIC %md
# MAGIC # Clean up notebook
# MAGIC Responsibilities of the notebook are:
# MAGIC 1. Read the jobs retention configuration for all tenants
# MAGIC 2. Remove all jobs which are older then the configured thresholds for specific tenant

# COMMAND ----------

import logging
from modules.unity_logging import LoggerConfiguration
from modules.configuration import GlobalConfiguration
from modules.databricks_utils import DatabricksJobManager

tenant_name = "shared"

logger = logging.getLogger(__name__)
LoggerConfiguration(tenant_name).configure(logger)

# COMMAND ----------

retention_periods = GlobalConfiguration.get_tenant_job_retention_period()
logger.info(f"Retention periods for job cleanup are: {retention_periods}")
DatabricksJobManager().cleanup_jobs(retention_periods)
