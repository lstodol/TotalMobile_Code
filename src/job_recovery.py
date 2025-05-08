# Databricks notebook source
import logging
from datetime import datetime
from modules.unity_logging import LoggerConfiguration
from modules.databricks_utils import AuditLogger

tenant_name = "shared"

logger = logging.getLogger(__name__)
LoggerConfiguration(tenant_name).configure(logger)

# COMMAND ----------

dbutils.widgets.text("layer", "")
dbutils.widgets.text("tenant", "")
dbutils.widgets.text("product", "")
dbutils.widgets.text("status", "")
dbutils.widgets.text("message", "")

layer = dbutils.widgets.get("layer")
tenant = dbutils.widgets.get("tenant")
product = dbutils.widgets.get("product")
status = dbutils.widgets.get("status")
message = dbutils.widgets.get("message")

if layer == "" or tenant == "" or product == "":
    logger.info("Invalid parameters for notebook.")
    dbutils.notebook.exit("Invalid parameters for notebook.")

# COMMAND ----------

def main():

    try:
        
        run_details = {}
        dts = datetime.now()

        ###Run details###
        logger.info(f"Preparing run details for the job audit entry...")
        run_details["job_name"] = f"{layer}_{tenant}_{product}_{dts}"
        run_details["job_id"] = 999
        run_details["start_time"] = ""
        run_details["end_time"] = ""
        run_details["run_page_url"] = ""
        run_details["number_of_tasks"] = 999
        run_details["status"] = f"{status}"
        run_details["error_desc"] = f"{message}"
        run_details["updated_by"] = "manual"

        ###audit entry###
        logger.info(f"Adding a manual entry to job audit table...")
        audit_logger = AuditLogger(tenant, product, layer, "insert")
        audit_logger.insert_job_audit_table(run_details)
        logger.info(f"Completed the manual entry to job audit table...")
        
    except Exception as e:
        # Handle the exception
        logger.error("An exception occurred " + str(e))
        raise Exception("An exception occurred " + str(e))

# COMMAND ----------

main()

# COMMAND ----------

audit_logger = AuditLogger(tenant, product, layer, "insert")
audit_logger.read_job_audit_table()
