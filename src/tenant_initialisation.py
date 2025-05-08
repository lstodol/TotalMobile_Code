# Databricks notebook source
# MAGIC %md
# MAGIC # Main notebook initialisating all tenants
# MAGIC Responsibilities of the notebook are:
# MAGIC 1. Mounts config container - to path <b>"mnt/config"</b>
# MAGIC 2. For all tenants mount tenant container - to path <b>"mnt/tenant_name"</b>
# MAGIC 3. For each tenant and its products - create DB structures
# MAGIC 4. Rewrite configs to env_config location
# MAGIC ## When it is executed? 
# MAGIC It is executed from the DevOps pipeline during code deployment. 

# COMMAND ----------

import logging

from modules.unity_logging import LoggerConfiguration
from modules.configuration import GlobalConfiguration

dbutils.widgets.text("tenant_name", "shared")
tenant_name = dbutils.widgets.get("tenant_name")

# COMMAND ----------

logger = logging.getLogger(__name__)
LoggerConfiguration(tenant_name).configure(logger)

config = GlobalConfiguration(True)

# COMMAND ----------

if tenant_name == "shared":
    tenant_name = None
    
config.init_tenants(tenant_name)
