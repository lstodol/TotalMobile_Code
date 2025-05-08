# Databricks notebook source
dbutils.widgets.text("run_id", "manual_run")
run_id = dbutils.widgets.get("run_id")

# COMMAND ----------

import fixpath
import logging
from modules.unity_logging import LoggerConfiguration

from test_utils import run_test_from_test_cases

from unit_test_bronze_layer import (
    TestBronzeLiveTableDeletesProcessing,
    TestBronzeLiveTableInsertsProcessing,
    TestBronzeLiveTableUpdatesProcessing,
    TestBronzeReferenceTableProcessing,
    TestBronzeHistoryTableProcessing
)

from unit_test_databricks_utils_auditlogger import TestAuditLogger
from unit_test_databricks_utils_databricksjobmanager import TestDatabricksJobManager
from unit_test_exceptions import TestExceptionCatcher
from unit_test_job_manager import TestJobManager
from unit_test_unity_logging import TestUnityLogging

from unit_test_authorisation import TestAuthorisation
from unit_test_configuration_utils import TestConfigurationUtils
from unit_test_configuration import TestConfiguration, TestCarelinkConfiguration, TestTotalMobileConfiguration, TestLiveSchedulerConfiguration

from hot.ingestor.tests.unit_test_interactor import TestIngestorInteractor
from hot.ingestor.tests.unit_test_processor import TestIngestorProcessor
from hot.ingestor.tests.unit_test_repository import TestIngestorRepository

from hot.domain.care.tests.unit_test_interactor import TestDomainCareInteractor
from hot.domain.care.tests.unit_test_processor import TestDomainCareProcessor
from hot.domain.care.tests.unit_test_repository import TestDomainCareRepository


test_cases_to_run = [
     TestBronzeLiveTableDeletesProcessing,
     TestBronzeLiveTableInsertsProcessing,
     TestBronzeLiveTableUpdatesProcessing,
     TestBronzeReferenceTableProcessing,
     TestBronzeHistoryTableProcessing,
     TestAuditLogger,
     TestDatabricksJobManager,
     TestExceptionCatcher,
     TestConfigurationUtils,
     TestAuthorisation,
     TestConfiguration, 
     TestCarelinkConfiguration,
     TestLiveSchedulerConfiguration,
     TestTotalMobileConfiguration,
     TestJobManager,
     TestUnityLogging,
     TestIngestorProcessor,
     TestIngestorInteractor,
     TestIngestorRepository,
     TestDomainCareInteractor,
     TestDomainCareProcessor,
     TestDomainCareRepository
]

# COMMAND ----------

tenant_name = "shared"

logger = logging.getLogger(__name__)
LoggerConfiguration(tenant_name).configure(logger)

results = run_test_from_test_cases(test_cases_to_run, run_id)

# COMMAND ----------

results
