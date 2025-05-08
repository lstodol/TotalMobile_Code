# Databricks notebook source
# MAGIC %md
# MAGIC ## Initialise demo tenant

# COMMAND ----------

# DBTITLE 1,Get relevant parameters
dbutils.widgets.text("tenant_name", "")
dbutils.widgets.text("product_name", "")

tenant_name = dbutils.widgets.get("tenant_name")
product_name = dbutils.widgets.get("product_name")

# COMMAND ----------

# DBTITLE 1,Initialize logging
import json, sys, os
import fixpath
import logging

sys.path.append('/Workspace/Repos/lukasz.stodolka@totalmobile.co.uk/analytics-unity-code/src')

from modules.unity_logging import LoggerConfiguration

logger = logging.getLogger(__name__)
LoggerConfiguration(tenant_name).configure(logger)

# COMMAND ----------

# DBTITLE 1,Trigger General Tenant Initialisation Notebook
context = json.loads(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
)
current_path = context["extraContext"]["notebook_path"].rsplit("/", 1)[0]
root_path = os.path.abspath(os.path.join(current_path, os.pardir))
#dirty fix!
root_path = root_path.replace("hot/domain/insight", "").rstrip("/")

init_tenant_notebook_path = f"{root_path}/tenant_initialisation"

logger.info("Initialising demo tenant...")

dbutils.notebook.run(init_tenant_notebook_path, 600, {"tenant_name": tenant_name})

logger.info("Initialising demo tenant...DONE")

# COMMAND ----------

# DBTITLE 1,Apply required schema changes
logger.info("Applying required schema changes...")

table_prefix = f"{tenant_name}.gold_{product_name}"
gold_location = f"/mnt/{tenant_name}/lakehouse/gold"

create_worker_dimension = f"""
    CREATE TABLE {table_prefix}_workerdimension (
        id INT,
        worker_reference STRING,
        WorkerID INT,
        Name STRING,
        load_date TIMESTAMP
    )
    USING DELTA
    LOCATION '{gold_location}/{table_prefix}_workerdimension'
"""

create_temp_worker_dimension = f"""
    CREATE TABLE  {table_prefix}_temp_workerdimension (
        id INT,
        worker_reference STRING,
        WorkerID INT,
        Name STRING,
        load_date TIMESTAMP
    )
    USING DELTA
    LOCATION '{gold_location}/{table_prefix}_temp_workerdimension'
"""

spark.sql(f"USE {tenant_name}")

table_exists = spark.catalog.tableExists(f"{table_prefix}_workerdimension")

if table_exists:
    temp_table_name = f"{table_prefix}_temp_workerdimension"
    temp_table_path = f'{gold_location}/{table_prefix}_temp_workerdimension'

    spark.sql(create_temp_worker_dimension)
    spark.sql(
        f"""
            INSERT INTO {temp_table_name}
            SELECT  id, worker_reference, WorkerID, Name, load_date FROM {table_prefix}_workerdimension
        """
    )

    spark.sql(f"DROP TABLE {table_prefix}_workerdimension")
    real_table_path = f'{gold_location}/{table_prefix}_workerdimension'
    dbutils.fs.rm(real_table_path, recurse=True)
    
    spark.sql(create_worker_dimension)
    spark.sql(
        f"""
            INSERT INTO {table_prefix}_workerdimension
            SELECT * FROM {temp_table_name}
        """
    )
    spark.sql(f"DROP TABLE IF EXISTS {temp_table_name}")
    dbutils.fs.rm(temp_table_path, True)
else:
    spark.sql(create_worker_dimension)

spark.sql(
    f"""
        CREATE TABLE IF NOT EXISTS {table_prefix}_sample (
            id INT,
            status INT,
            start_datetime_utc TIMESTAMP,
            end_datetime_utc TIMESTAMP
        )
        USING DELTA
        LOCATION '{gold_location}/{table_prefix}_sample'
    """
)

logger.info("Performing cleanup...")

dbutils.fs.rm('dbfs:/user/hive/warehouse/mnt/{tenant_name}/lakehouse/gold/{tenant_name}.gold_{product_name}_workerdimension', True)

logger.info("Applying required schema changes...DONE")

# COMMAND ----------

# DBTITLE 1,Create demo job
logger.info("Executing job wrapper...")

hot_wrapper_path = f"{root_path}/job_wrapper_hot"
dbutils.notebook.run(hot_wrapper_path, 180)

logger.info("Executing job wrapper...DONE")

# COMMAND ----------

# DBTITLE 1,Modifying required settings
import requests
import json

host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()

token = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
)

browser_hostname = (
    dbutils.secrets.get(scope="analytics", key="dbx-workspace-url")
)
job_name = f"ingestor_{tenant_name}_{product_name}"

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
}

def list_jobs():
    url = f"https://{browser_hostname}/api/2.1/jobs/list"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def get_job_id_by_name(job_name):
    jobs = list_jobs().get("jobs", [])
    for job in jobs:
        if job["settings"]["name"] == job_name:
            return job["job_id"]
    return None


job_id = get_job_id_by_name(job_name)

if job_id:
    logger.info(f"The job id to be modified is: {job_id}.")
else:
    logger.warning(f"Job with name: {job_name} not found.")


def update_workflow_settings(job_id, new_settings):
    url = f"https://{browser_hostname}/api/2.1/jobs/update"
    data = {"job_id": job_id, "new_settings": new_settings}
    response = requests.post(url, headers=headers, data=json.dumps(data))
    response.raise_for_status()
    return response.json()

new_settings = {
    "tasks": [
        {
            "task_key": f"{tenant_name}_{product_name}",
            "notebook_task": {"notebook_path": f"{root_path}/hot/domain/insight/ingestor-demo/main"},
        }
    ],
}

if job_id:
    try:
        update_response = update_workflow_settings(job_id, new_settings)
        logger.info("Update response: %s", json.dumps(update_response, indent=2))
    except requests.exceptions.HTTPError as e:
        logger.error("Error whilst updating job settings: %s", str(e))
else:
    logger.warning("No job ID found to update the settings.")
