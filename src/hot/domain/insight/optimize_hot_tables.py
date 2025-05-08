# Databricks notebook source
# MAGIC %md
# MAGIC ### Import relevant modules and enable logging

# COMMAND ----------

import logging
import fixpath

from modules.unity_logging import LoggerConfiguration
from pyspark.sql.functions import *
from hot.modules.configuration_hot import GlobalConfigurationHot

logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform OPTIMIZE for all hot tenants and products

# COMMAND ----------

hot_tenant_dict = ConfigurationUtils.read_config(
    GlobalConfigurationHot.ENV_TENANT_CONFIG_PATH_HOT
)
logger.info(f"Tenant config read completed.")

for tenant, products in hot_tenant_dict.items():
    database_name = tenant
    logger.info(f"Processing tenant: {tenant}")

    all_tables_df = spark.sql(f"SHOW TABLES IN {database_name}")

    for product in products:
        logger.info(f"Processing product: {product}")

        filtered_tables_df = all_tables_df \
            .filter(~all_tables_df.tableName.endswith('_audit')) \
            .filter(~all_tables_df.tableName.contains('thinslice')) \
            .filter(all_tables_df.tableName.contains(product))

        for row in filtered_tables_df.collect():
            table_name = row['tableName']
            full_table_name = f"{database_name}.{table_name}"
            
            logger.info(f"Optimizing table: {full_table_name}")
            # spark.sql(f"OPTIMIZE {full_table_name}")
