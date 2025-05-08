from unittest import TestCase
from unittest.mock import patch, call
import fixpath
from modules.databricks_utils import AuditLogger
from databricks.sdk.service import jobs


class TestAuditLogger(TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.tenant = "unittestdbxutilsauditlogtenant1"
        cls.product = "carelink"
        cls.layer = "bronze"

        cls.mount_location = "/mnt"
        cls.job_audit_table_name = f"{cls.tenant}.{cls.layer}_{cls.product}_job_audit"
        cls.job_audit_table_loc = (
            f"{cls.mount_location}/{cls.tenant}/{cls.layer}/{cls.job_audit_table_name}"
        )
        cls.task_audit_table_name = f"{cls.tenant}.{cls.layer}_{cls.product}_task_audit"
        cls.task_audit_table_loc = (
            f"{cls.mount_location}/{cls.tenant}/{cls.layer}/{cls.task_audit_table_name}"
        )
        
        cls.job_run_output = jobs.Run(
            job_id=1,
            run_id=1,
            run_name="runname",
            tasks=[
                jobs.RunTask(run_id=10),
                jobs.RunTask(run_id=11),
                jobs.RunTask(run_id=12),
            ],
            run_page_url="https://runpageurl.com",
            start_time=1701422922910,
            end_time=1701423922910,
            state=jobs.RunState(
                life_cycle_state=jobs.RunLifeCycleState.TERMINATED,
                queue_reason=None,
                result_state=jobs.RunResultState.SUCCESS,
                state_message="state message",
                user_cancelled_or_timedout=False,
            ),
        )
        
        cls.run_output = jobs.RunOutput(metadata=cls.job_run_output)
        
        cls.run_details = {
            "job_id": "",
            "job_name": "",
            "job_run_id": "",
            "run_name": "",
            "run_id": "",
            "start_time": "",
            "end_time": "",
            "run_page_url": "",
            "status": "",
            "error_desc": "",
            "updated_by": ""
        }
        
        cls.job_output = jobs.BaseJob(
            job_id=1,
            settings=jobs.JobSettings(
                name="jobname",
                tasks=[
                    jobs.Task(task_key="10"),
                    jobs.Task(task_key="11"),
                    jobs.Task(task_key="12"),
                ],
            ),
        )

    ### Testing Job audit table name
    def test_job_audit_table_name(self):
        audit_logger = AuditLogger(self.tenant, self.product, self.layer, "some_action")
        self.assertEqual(
            self.job_audit_table_name,
            audit_logger.job_audit_table_name,
            msg="Job audit table has wrong name pattern.",
        )

    ### Testing Task audit table name
    def test_task_audit_table_name(self):
        audit_logger = AuditLogger(self.tenant, self.product, self.layer, "some_action")
        self.assertEqual(
            self.task_audit_table_name,
            audit_logger.task_audit_table_name,
            msg="Task audit table has wrong name pattern.",
        )

    ### Testing get_run_details method
    def test_get_run_details(self):
        # arrange
        audit_logger = AuditLogger(self.tenant, self.product, self.layer, "some_action")

        # act
        run_details = audit_logger.get_run_details(self.job_run_output)

        # assert
        self.assertEqual(
            self.job_run_output.job_id,
            run_details["job_id"],
            msg="Job id is not mapped correctly.",
        )
        self.assertEqual(
            self.job_run_output.run_id,
            run_details["run_id"],
            msg="Run id is not mapped correctly.",
        )
        self.assertEqual(
            self.job_run_output.run_name,
            run_details["run_name"],
            msg="Run name id is not mapped correctly.",
        )
        self.assertEqual(
            3, run_details["number_of_tasks"], msg="Number of tasks is not correct."
        )
        self.assertEqual(
            self.job_run_output.run_page_url,
            run_details["run_page_url"],
            msg="Run page url is not mapped correctly.",
        )
        self.assertEqual(
            "2023-12-01 09:28:42.910000",
            run_details["start_time"],
            msg="Start time is not mapped correctly.",
        )
        self.assertEqual(
            "unity-app",
            run_details["updated_by"],
            msg="Updated by is wrongly configured.",
        )

    ### Test get_run details for a completed job
    def test_get_run_details_for_completed(self):
        # arrange
        audit_logger = AuditLogger(self.tenant, self.product, self.layer, "some_action")
        
        job_run_output =  jobs.Run.from_dict(self.job_run_output.as_dict())
        job_run_output.state = jobs.RunState(
            life_cycle_state=jobs.RunLifeCycleState.TERMINATED,
            result_state=jobs.RunResultState.SUCCESS,
            state_message="state message",
        )

        # act
        run_details = audit_logger.get_run_details(job_run_output)

        # assert
        self.assertEqual(
            "2023-12-01 09:45:22.910000",
            run_details["end_time"],
            msg="Start time is not mapped correctly.",
        )
        self.assertEqual(
            "SUCCESS", run_details["status"], msg="The status is not correct."
        )
        self.assertEqual(
            "state message",
            run_details["error_desc"],
            msg="Updated by is wrongly configured.",
        )

    ### Test get_run details for a running job
    def test_get_run_details_for_running(self):
        # arrange
        audit_logger = AuditLogger(self.tenant, self.product, self.layer, "some_action")
        
        job_run_output =  jobs.Run.from_dict(self.job_run_output.as_dict())
        
        job_run_output.state = jobs.RunState(
            life_cycle_state=jobs.RunLifeCycleState.RUNNING,
            result_state=jobs.RunResultState.SUCCESS,
            state_message="state message",
        )
        # act
        run_details = audit_logger.get_run_details(job_run_output)

        # assert
        self.assertEqual(
            "", run_details["end_time"], msg="Start time is not mapped correctly."
        )
        self.assertEqual(
            "RUNNING", run_details["status"], msg="The status is not correct."
        )
        self.assertEqual(
            "", run_details["error_desc"], msg="Updated by is wrongly configured."
        )

    ### Test job_run_update
    @patch.object(AuditLogger, "task_run_update")
    @patch.object(AuditLogger, "update_job_audit_table")
    def test_job_run_update(self, mocked_update_job_audit_table, mocked_task_run_update):
        # arrange
        audit_logger = AuditLogger(self.tenant, self.product, self.layer, "some_action")
        expected_run_details = {
            "job_id": 1,
            "run_id": 1,
            "run_name": "runname",
            "number_of_tasks": 3,
            "run_page_url": "https://runpageurl.com",
            "start_time": "2023-12-01 09:28:42.910000",
            "updated_by": "unity-app",
            "end_time": "2023-12-01 09:45:22.910000",
            "status": "SUCCESS",
            "error_desc": "state message",
        }

        with patch.object(
            jobs.JobsAPI, "get_run", return_value=self.job_run_output
        ) as mocked_get_run:
            # act
            audit_logger.job_run_update(job_run_id=1)

        # assert
        mocked_get_run.assert_called_with(run_id=expected_run_details["run_id"])
        mocked_update_job_audit_table.assert_called_once_with(
            expected_run_details
        )
        mocked_task_run_update.assert_called_once_with(
            self.job_run_output,
            expected_run_details["run_name"],
            expected_run_details["run_id"],
        )

    ### Test task_run_update
    @patch.object(AuditLogger, "update_task_audit_table")
    def test_task_run_update(self, mocked_update_task_audit_table):
        # arrange
        audit_logger = AuditLogger(self.tenant, self.product, self.layer, "some_action")
        expected_run_details = {
            "job_id": 1,
            "run_id": 1,
            "job_name": 1,
            "job_run_id": 1,
            "run_name": "runname",
            "number_of_tasks": 3,
            "run_page_url": "https://runpageurl.com",
            "start_time": "2023-12-01 09:28:42.910000",
            "updated_by": "unity-app",
            "end_time": "2023-12-01 09:45:22.910000",
            "status": "SUCCESS",
            "error_desc": "state message",
        }

        with patch.object(
            jobs.JobsAPI, "get_run_output", return_value=self.run_output
        ) as mocked_get_run_output:
            # act
            audit_logger.task_run_update(self.job_run_output, 1, 1)

        # assert
        mocked_get_run_output.assert_has_calls(
            [call(run_id=10), call(run_id=11), call(run_id=12)]
        )
        mocked_update_task_audit_table.assert_has_calls(
            [
                call([expected_run_details, expected_run_details, expected_run_details])
            ]
        )

    ### Test get_job_details
    def test_get_job_details(self):
        # arrange
        audit_logger = AuditLogger(self.tenant, self.product, self.layer, "some_action")
        expected_job_details = {
            "job_id": 1,
            "job_name": "jobname",
            "number_of_tasks": 3,
            "status": "CREATED"
        }

        # act
        job_details = audit_logger.get_job_details(self.job_output)

        # assert
        self.assertEqual(
            expected_job_details["job_id"],
            job_details["job_id"],
            msg="Job id is not mapped correctly.",
        )
        self.assertEqual(
            expected_job_details["job_name"],
            job_details["job_name"],
            msg="Job name is not mapped correctly.",
        )
        self.assertEqual(
            expected_job_details["number_of_tasks"], job_details["number_of_tasks"], msg="The number of tasks is not correct."
        )
        self.assertEqual(
            expected_job_details["status"],
            job_details["status"],
            msg="Status is wrongly configured.",
        )

    ### Test job_create_update
    @patch.object(AuditLogger, "task_create_update")
    @patch.object(AuditLogger, "insert_job_audit_table")
    def test_job_create_update(self, mocked_insert_job_audit_table, mocked_task_create_update):
        # arrange
        audit_logger = AuditLogger(self.tenant, self.product, self.layer, "some_action")
        expected_job_details = {
            "job_id": 1,
            "job_name": "jobname",
            "start_time": "",
            "end_time": "",
            "run_page_url": "",
            "number_of_tasks": 3,
            "updated_by": "unity-app",
            "status": "CREATED",
            "error_desc": "",
        }

        with patch.object(
            jobs.JobsAPI, "get", return_value=self.job_output
        ) as mocked_get_job_data:
            # act
            audit_logger.job_create_update(job_id=1)

        # assert
        mocked_get_job_data.assert_called_with(job_id=expected_job_details["job_id"])
        mocked_insert_job_audit_table.assert_called_once_with(expected_job_details)
        mocked_task_create_update.assert_called_once_with(self.job_output)


    ### Test task_create_update
    @patch.object(AuditLogger, "insert_task_audit_table")
    def test_task_create_update(self, mocked_insert_task_audit_table):
        # arrange
        audit_logger = AuditLogger(self.tenant, self.product, self.layer, "some_action")
        expected_task_details_1 = {
            "job_id": 1,
            "job_name": "jobname",
            "start_time": "",
            "end_time": "",
            "run_page_url": "",
            "number_of_tasks": 3,
            "updated_by": "unity-app",
            "status": "CREATED",
            "error_desc": "",
            "task_name": "10"
        }

        expected_task_details_2 = {
            "job_id": 1,
            "job_name": "jobname",
            "start_time": "",
            "end_time": "",
            "run_page_url": "",
            "number_of_tasks": 3,
            "updated_by": "unity-app",
            "status": "CREATED",
            "error_desc": "",
            "task_name": "11"
        }
        expected_task_details_3 = {
            "job_id": 1,
            "job_name": "jobname",
            "start_time": "",
            "end_time": "",
            "run_page_url": "",
            "number_of_tasks": 3,
            "updated_by": "unity-app",
            "status": "CREATED",
            "error_desc": "",
            "task_name": "12"
        }
        
        audit_logger.task_create_update(self.job_output)

        # assert
        mocked_insert_task_audit_table.assert_has_calls(
            [
                call([expected_task_details_1, expected_task_details_2, expected_task_details_3]),
            ]
        )


    ### Test update_job_audit_table for run updates
    @patch.object(AuditLogger, 'upd_job_audit')
    @patch.object(AuditLogger, 'run_id_upd_job_audit') 
    def test_update_job_audit_table(self, mocked_run_id_upd_job_audit, mocked_upd_job_audit):
        
        audit_logger = AuditLogger(self.tenant, self.product, self.layer, "update")
        audit_logger.update_job_audit_table(self.run_details)

        # assertions
        mocked_upd_job_audit.assert_called()
        mocked_run_id_upd_job_audit.assert_not_called()

    ### Test update_job_audit_table for runid update
    @patch.object(AuditLogger, 'upd_job_audit')
    @patch.object(AuditLogger, 'run_id_upd_job_audit') 
    def test_run_id_update_job_audit_table(self, mocked_run_id_upd_job_audit, mocked_upd_job_audit):

        audit_logger = AuditLogger(self.tenant, self.product, self.layer, "run_id_update")
        audit_logger.update_job_audit_table(self.run_details)
        
        # assertions
        mocked_run_id_upd_job_audit.assert_called()
        mocked_upd_job_audit.assert_not_called()

    ### Test update_task_audit_table for run updates
    @patch.object(AuditLogger, 'upd_task_audit')
    @patch.object(AuditLogger, 'run_id_upd_task_audit') 
    def test_update_task_audit_table(self, mocked_run_id_upd_task_audit, mocked_upd_task_audit):
        
        audit_logger = AuditLogger(self.tenant, self.product, self.layer, "update")
        audit_logger.update_task_audit_table([self.run_details])

        # assertions
        mocked_upd_task_audit.assert_called()
        mocked_run_id_upd_task_audit.assert_not_called()

    ### Test update_task_audit_table for runid updates
    @patch.object(AuditLogger, 'upd_task_audit')
    @patch.object(AuditLogger, 'run_id_upd_task_audit') 
    def test_run_id_update_task_audit_table(self, mocked_run_id_upd_task_audit, mocked_upd_task_audit):
        
        audit_logger = AuditLogger(self.tenant, self.product, self.layer, "run_id_update")
        audit_logger.update_task_audit_table([self.run_details])

        # assertions
        mocked_run_id_upd_task_audit.assert_called()        
        mocked_upd_task_audit.assert_not_called()