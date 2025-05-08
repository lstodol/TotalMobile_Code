# Databricks notebook source
dbutils.widgets.text(name='tenant_name', label='tenant_name', defaultValue='')
dbutils.widgets.text(name='product_name', label='product_name', defaultValue='')
dbutils.widgets.text(name='event_hub_namespace', label='event_hub_namespace', defaultValue='')
dbutils.widgets.text(name='entity_name', label='entity_name', defaultValue='')

tenant_name = dbutils.widgets.get('tenant_name')
product_name = dbutils.widgets.get('product_name')
event_hub_namespace = dbutils.widgets.get('event_hub_namespace')
entity_name = dbutils.widgets.get('entity_name')

# COMMAND ----------

import logging
import fixpath

from hot.ingestor.storage.repository import new_repository
from hot.ingestor.usecase.interactor import new_interactor
from hot.ingestor.handler.processor import new_processor
from modules.unity_logging import LoggerConfiguration
from modules.authorisation import Authorisation

logger = logging.getLogger(__name__)
LoggerConfiguration(tenant_name).configure(logger)

# COMMAND ----------

if __name__ == "__main__":
    try:
        connection_string = dbutils.secrets.get(Authorisation.SECRET_SCOPE, f"sas-{event_hub_namespace}-{entity_name}-listen-evh-connection")

        repository = new_repository(connection_string=connection_string, spark_session=spark)
        interactor = new_interactor(repository=repository, spark_session=spark)
        processor = new_processor(interactor=interactor)

        processor.handler(tenant_name, product_name, entity_name)
        
    except Exception as e:
        logger.error(f"Ingestor error for tenant: {tenant_name}, product: {product_name}, entity: {entity_name}. Exception details: {e}")
        raise e
