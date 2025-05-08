import unittest
import xmlrunner
import logging
import uuid
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

logger = logging.getLogger(__name__)


def run_test_from_test_cases(
    test_cases: list[unittest.TestCase],
    run_id:str = "manual_run"
) -> unittest.TestResult:
    """This helper function can be used to load and run unittest TestCases
    in a Databricks notebook in a convenient way."""
    tests_in_suite = [
        unittest.TestLoader().loadTestsFromTestCase(test_case)
        for test_case in test_cases
    ]
    
    dbfs_output = f"dbfs:/analytics/test_results/{run_id}"
    guid = uuid.uuid4()
    local_output = f"{Path('.').absolute()}/.local/{guid}"
    logger.info(local_output)

    suite = unittest.TestSuite(tests_in_suite)
    runner = xmlrunner.XMLTestRunner(output=local_output)
    results = runner.run(suite)

    dbutils = DBUtils(SparkSession.builder.getOrCreate())

    dbutils.fs.cp(f"file:{local_output}", dbfs_output, True)
    #clean up up local xml runner files
    dbutils.fs.rm(f"file:{local_output}", True)

    return results