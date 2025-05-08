# Databricks notebook source
dbutils.widgets.text('tenant_name', "")
dbutils.widgets.text('domain_name', "")
dbutils.widgets.text('table_name', "")
dbutils.widgets.text('data_layer', "")

tenant_name = dbutils.widgets.get('tenant_name')
domain_name = dbutils.widgets.get('domain_name')
table_name = dbutils.widgets.get('table_name')
data_layer = dbutils.widgets.get('data_layer')

# COMMAND ----------

spark.conf.set("spark.databricks.delta.changeDataFeed.timestampOutOfRange.enabled", "true")

# COMMAND ----------

import logging
import fixpath

from storage.repository import new_repository
from usecase.interactor import new_interactor
from handler.processor import new_processor
from modules.unity_logging import LoggerConfiguration

logger = logging.getLogger(__name__)
LoggerConfiguration(tenant_name).configure(logger)


# COMMAND ----------

if __name__ == "__main__":
    r = new_repository()
    i = new_interactor(r)
    p = new_processor(i)

    try:
        p.handler(tenant_name, domain_name, table_name, data_layer)
    except Exception as e:
        raise e
