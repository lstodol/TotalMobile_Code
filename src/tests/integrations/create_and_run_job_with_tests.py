# Databricks notebook source
# DBTITLE 1,parameters
dbutils.widgets.text("run_id", "manual_run")
run_id = dbutils.widgets.get("run_id")

# COMMAND ----------

# DBTITLE 1,imports
import logging
import json
import fixpath
import time
from modules.unity_logging import LoggerConfiguration
from databricks.sdk.service import jobs
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# DBTITLE 1,executions
tenant_name = "shared"

logger = logging.getLogger(__name__)
LoggerConfiguration(tenant_name).configure(logger)


# COMMAND ----------

all_test_cases = [
    "TestTenantInitialization",
    "TestJobMonitoring",
    "TestJobWrapperIntegration",
    "TestIngestor",
    "TestCare",
    "TestDatabricksJobManager",
    "TestJobCleanup",
    "TestJobRecovery",
    "TestLoadBronzeTable",
    "TestAuthorisation",
    "TestUnityLogging",
    "TestAuditLogger",
]

w = WorkspaceClient()
cluster = [item for item in w.clusters.list() if item.cluster_name == "main_runner"]
cluster_id = cluster[0].cluster_id
context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
current_path = context['extraContext']['notebook_path'].rsplit('/', 1)[0]
runner_path = f"{current_path}/runner"

tasks = []
for test_case in all_test_cases:

    task = jobs.Task(
                    description=f"{test_case}_{run_id}",
                    task_key=f"{test_case}_{run_id}",
                    existing_cluster_id=f"{cluster_id}",
                    notebook_task=jobs.NotebookTask(
                        notebook_path=f"{runner_path}",
                        base_parameters={
                            "run_id": f"{run_id}",
                            "test_case": f"{test_case}",
                        },
                    ),
                    max_retries=0,
                    timeout_seconds=0)
    tasks.append(task)

job = w.jobs.create(
            name=f"{run_id}_integration_test",
            tasks=tasks,
        )

def check_job_status(run_id):
    run = w.jobs.get_run(run_id=run_id)
    logger.info(f"checking status: {run.state.life_cycle_state}")
    if (run.state.life_cycle_state == jobs.RunLifeCycleState.TERMINATED 
        or run.state == jobs.RunLifeCycleState.SKIPPED
        or run.state.life_cycle_state == jobs.RunLifeCycleState.INTERNAL_ERROR) :
        return True
    return False

try:
    start_time = time.time()
    job_run = w.jobs.run_now(job_id=job.job_id)

    while not check_job_status(job_run.run_id):
        time.sleep(10)  # Wait for 10 seconds before checking again

    end_time = time.time()
    execution_time = end_time - start_time
    logger.info(f"Job finished in {execution_time:.2f} seconds")

    run = w.jobs.get_run(run_id=job_run.run_id)
    if run.state.life_cycle_state == jobs.RunLifeCycleState.INTERNAL_ERROR:
        raise Exception(f"Job failed with state {run.state.life_cycle_state}")

except Exception as e:
    logger.error(f"Job failed with error {e}")
    raise e

w.jobs.delete(job.job_id)
