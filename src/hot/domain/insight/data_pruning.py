# Databricks notebook source
# MAGIC %md
# MAGIC ### Import relevant modules and enable logging

# COMMAND ----------

import logging
import fixpath

from modules.unity_logging import LoggerConfiguration
from hot.modules.configuration_hot import GlobalConfigurationHot
from modules.configuration_utils import ConfigurationUtils

from pyspark.sql.functions import col, row_number, min as spark_min
from pyspark.sql.types import DateType
from pyspark.sql import Window
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform data pruning for all tenants 

# COMMAND ----------

tenant_domain_dict = ConfigurationUtils.read_config(
    GlobalConfigurationHot.TENANT_DOMAIN_CONFIG_PATH_HOT
)
logger.info(f"Tenant domain config read completed.")

active_insight_tenants = [
    tenant
    for tenant in tenant_domain_dict
    if tenant["Domain"] == "insight" and tenant["IsActive"] == True
]
print(active_insight_tenants)

def downsample_by_day(df):
    
    df = df.withColumn("partition_date", col("version_start").cast(DateType()))

    # pass PK ? 
    # Create window specifications to find the latest version per day for each shift_id AND update the version_start to the earliest one.
    window_spec_max = Window.partitionBy("shift_id", "partition_date").orderBy(col("version_start").desc())
    window_spec_min = Window.partitionBy("shift_id", "partition_date")

    # Use row_number to rank rows within each partition, and get the latest entry for each shift per day
    df_latest = df.withColumn("row_num", row_number().over(window_spec_max)) \
                .withColumn("commit_start", spark_min("version_start").over(window_spec_min)) \
                .filter(col("row_num") == 1) \
                .drop("row_num", "partition_date","version_start") \
                .withColumnRenamed("commit_start", "version_start") \
                .select(
                    "shift_id", "kpi", "kpi_value", "version_start", "version_end"
                )
    
    return df_latest

def process_tenant_tables(tenant_name, data_horizon_days, truncate_age_days):
    
    tables = spark.sql(f"SHOW TABLES IN {tenant_name}") \
        .filter((col("isTemporary") == False) & (col("tableName").like('%testdatapruning%'))) \
        .collect()
    
    today = datetime.now()
    horizon_date = today - timedelta(days=data_horizon_days)
    truncate_date = today - timedelta(days=truncate_age_days)

    for table in tables:
        table_name = table.tableName
        
        df = spark.sql(f"SELECT * FROM {tenant_name}.{table_name}")
        
        # Find data beyond the truncation date
        df_filtered = df.filter(col('version_start') > truncate_date)

        # Apply down-sampling if version_start is older than the data horizon
        df_to_downsample = df_filtered.filter(col('version_start') < horizon_date)

        df_retained = df_filtered.filter(col('version_start') >= horizon_date)
        # display(df_retained)

        # Down-sample by day
        df_downsampled = downsample_by_day(df_to_downsample)
        # display(df_downsampled)

        # Union the down-sampled data and retained data
        df_final = df_downsampled.unionByName(df_retained)
        display(df_final)

        # Save the down-sampled data back to Hive (overwrite the original table)
        # df_final.write.mode("overwrite").saveAsTable(f"{tenant_name}.{table_name}")

        # Remove data older than a certain threshold
        # spark.sql(f"DELETE FROM {tenant_name}.{table_name} WHERE version_start < '{truncate_date}'")

for tenant in active_insight_tenants:
    tenant_name = tenant["TenantName"]
    data_horizon_days = tenant["DataHorizonDays"]
    truncate_age_days = tenant["TruncateAgeDays"]
    
    print(f"Data pruning for tenant: {tenant_name} in progress...")
    process_tenant_tables(tenant_name, data_horizon_days, truncate_age_days)

print("Data pruning process completed for all active tenants.")
