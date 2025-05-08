# Databricks notebook source
dbutils.widgets.text('tenant_name', "")
dbutils.widgets.text('domain_name', "")
dbutils.widgets.text('table_name', "")
dbutils.widgets.text('primary_keys', "")
dbutils.widgets.text('data_layer', "")

tenant_name = dbutils.widgets.get('tenant_name')
domain_name = dbutils.widgets.get('domain_name')
table_name = dbutils.widgets.get('table_name')
primary_keys = dbutils.widgets.get('primary_keys')
data_layer = dbutils.widgets.get('data_layer')

# COMMAND ----------

import fixpath
import logging

from hot.domain.care.storage.repository import new_repository
from hot.domain.care.usecase.interactor import new_interactor
from hot.domain.care.handler.processor import new_processor
from modules.unity_logging import LoggerConfiguration


logger = logging.getLogger(__name__)
LoggerConfiguration(tenant_name).configure(logger)

# COMMAND ----------

if __name__ == "__main__":

    r = new_repository()
    i = new_interactor(r)
    p = new_processor(i)

    p.handler(tenant_name, domain_name, table_name, primary_keys, data_layer)
