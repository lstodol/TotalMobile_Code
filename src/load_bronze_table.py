# Databricks notebook source
import logging

from modules.unity_logging import LoggerConfiguration
from modules.bronze_layer import IngestBronzeTable

# COMMAND ----------

dbutils.widgets.text("tenant_name", "")
dbutils.widgets.text("product_name", "")
dbutils.widgets.text("schema_name", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("table_type", "")
dbutils.widgets.text("primary_key", "")
dbutils.widgets.text("incremental_column", "")


tenant_name = dbutils.widgets.get("tenant_name")
product_name = dbutils.widgets.get("product_name")
schema_name = dbutils.widgets.get("schema_name")
table_name = dbutils.widgets.get("table_name")
table_type = dbutils.widgets.get("table_type")
primary_key = dbutils.widgets.get("primary_key")
incremental_column = dbutils.widgets.get("incremental_column")

# COMMAND ----------

logger = logging.getLogger(__name__)
LoggerConfiguration(tenant_name).configure(logger)

# COMMAND ----------

ingestion = IngestBronzeTable(
    tenant_name, product_name, schema_name, table_name, table_type, primary_key, incremental_column
)
ingestion.load()
