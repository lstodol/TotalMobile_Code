# Databricks notebook source
# DBTITLE 1,parameters
dbutils.widgets.text("run_id", "manual_run")
run_id = dbutils.widgets.get("run_id")

dbutils.widgets.text("test_case", "")
test_case = dbutils.widgets.get("test_case")

# COMMAND ----------

# DBTITLE 1,imports
import fixpath
import logging
from modules.unity_logging import LoggerConfiguration
from test_utils import run_test_from_test_cases
from int_test_databricks_utils_auditlogger import TestAuditLogger
from int_test_databricks_utils_databricksjobmanager import TestDatabricksJobManager
from int_test_job_cleanup import TestJobCleanup
from int_test_job_monitoring import TestJobMonitoring
from int_test_job_recovery import TestJobRecovery
from int_test_tenant_initialisation import TestTenantInitialization
from int_test_load_bronze_table import TestLoadBronzeTable
from int_test_job_wrapper import TestJobWrapperIntegration
from int_test_authorisation import TestAuthorisation
from int_test_unity_logging import TestUnityLogging
from hot.ingestor.tests.int_test_ingestor import TestIngestor
from hot.domain.care.tests.int_test_care import TestCare

all_test_cases = [
    TestJobMonitoring,
    TestJobWrapperIntegration,    
    TestIngestor,
    TestCare,
    TestDatabricksJobManager,
    TestJobCleanup,
    TestJobRecovery,
    TestTenantInitialization,
    TestLoadBronzeTable,
    TestAuthorisation,
    TestUnityLogging,
    TestAuditLogger,
]

test_cases_to_run = [tc for tc in all_test_cases if test_case in tc.__name__ or test_case == ""]

# COMMAND ----------

# DBTITLE 1,executions
tenant_name = "shared"

logger = logging.getLogger(__name__)
LoggerConfiguration(tenant_name).configure(logger)

results = run_test_from_test_cases(test_cases_to_run, run_id)

# COMMAND ----------

results
