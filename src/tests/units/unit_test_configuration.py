import json
from unittest import TestCase
from unittest.mock import patch, call, MagicMock, ANY
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from modules.configuration import (
    CarelinkConfiguration,
    GlobalConfiguration,
    LiveSchedulerConfiguration,
    TotalMobileConfiguration,
    DatamartConfiguration,
) 
from modules.authorisation import Authorisation
from modules.exceptions import ProductConfigurationNotFoundException
from modules.databricks_utils import AuditLogger


class TestConfiguration(TestCase):

    @classmethod
    def setUpClass(cls):
        print("Testing TestConfiguration - setUpClass...")

        cls.spark = SparkSession.builder.getOrCreate()
        cls.dbutils = DBUtils(cls.spark)
        cls.test_folder = "/TestConfiguration"
        cls.tenant_config = (
            f"{cls.test_folder}/config/tenant_config/tenant_product.json"
        )
        cls.dbutils.fs.mkdirs(cls.test_folder)
        cls.database_folder = "lakehouse"
        cls.active_tenant = "tenant1"
        cls.active_tenant2 = "tenant3"
        cls.inactive_tenant = "tenant2"
        cls.product = "carelink"
        config = [
            {
                "TenantName": cls.active_tenant,
                "ProductName": cls.product,
                "IsActive": True,
                "Jobs": {
                    "NotificationDestinations": [],
                    "NotificationRecipients": ["piotr.kwiatkowski@totalmobile.co.uk"],
                    "JobDurationThreshold": 600,
                    "JobTimeoutThreshold": 1200,
                    "JobRetentionPeriodInDays": 3,
                    "ClusterName": "main_runner",
                    "TasksParallelStreams": 8,
                },
            },
            {
                "TenantName": cls.active_tenant2,
                "ProductName": cls.product,
                "IsActive": True,
            },
            {
                "TenantName": cls.inactive_tenant,
                "ProductName": cls.product,
                "IsActive": False,
            },
        ]

        cls.dbutils.fs.put(cls.tenant_config, json.dumps(config), overwrite=True)

        cls.global_config = f"{cls.test_folder}/config/global_config/jobs.json"
        global_config = {
            "NotificationDestinations": [],
            "NotificationRecipients": [
                "piotr.kwiatkowski@totalmobile.co.uk",
                "lukasz.stodolka@totalmobile.co.uk",
            ],
            "JobDurationThreshold": 300,
            "JobTimeoutThreshold": 600,
            "JobRetentionPeriodInDays": 5,
            "ClusterName": "main_runner",
            "TasksParallelStreams": 10,
        }

        cls.dbutils.fs.put(cls.global_config, json.dumps(global_config), overwrite=True)

        print("Testing TestConfiguration - setUpClass... DONE")

    def test_static_variables_mount_prefix(self):
        self.assertEqual(
            "/mnt", GlobalConfiguration.MOUNT_PREFIX, msg="prefix is not /mnt."
        )

    def test_static_variables_config_container(self):
        self.assertEqual(
            "config",
            GlobalConfiguration.CONFIG_CONTAINER,
            msg="config container is wrongly configured.",
        )

    def test_static_variables_config_path(self):
        self.assertEqual(
            "/mnt/config",
            GlobalConfiguration.CONFIG_PATH,
            msg="config path is wrongly configured.",
        )

    def test_static_variables_env_config_folder_path(self):
        self.assertEqual(
            "/mnt/config/env_config",
            GlobalConfiguration.ENV_CONFIG_FOLDER_PATH,
            msg="env_config_folder_path is wrongly configured.",
        )

    def test_static_variables_product_config_folder_path(self):
        self.assertEqual(
            "/mnt/config/product_config",
            GlobalConfiguration.PRODUCT_CONFIG_FOLDER_PATH,
            msg="product_config_folder_path is wrongly configured.",
        )

    def test_static_variables_global_config_folder_path(self):
        self.assertEqual(
            "/mnt/config/global_config",
            GlobalConfiguration.GLOBAL_CONFIG_FOLDER_PATH,
            msg="global_config_folder_path is wrongly configured.",
        )

    def test_static_variables_tenant_config_path(self):
        self.assertEqual(
            "/mnt/config/tenant_config/tenant_product.json",
            GlobalConfiguration.TENANT_CONFIG_PATH,
            msg="tenant_config_path is wrongly configured.",
        )

    def test_static_variables_global_jobs_config_path(self):
        self.assertEqual(
            "/mnt/config/global_config/jobs.json",
            GlobalConfiguration.GLOBAL_JOBS_CONFIG_PATH,
            msg="global_jobs_config_path is wrongly configured.",
        )

    def test_static_variables_env_tenant_config_path(self):
        self.assertEqual(
            "/mnt/config/env_config/tenant_config.json",
            GlobalConfiguration.ENV_TENANT_CONFIG_PATH,
            msg="env_tenant_config_path is wrongly configured.",
        )

    def test_static_variables_env_product_config_path(self):
        self.assertEqual(
            "/mnt/config/env_config/product_config.json",
            GlobalConfiguration.ENV_PRODUCT_CONFIG_PATH,
            msg="env_product_config_path is wrongly configured.",
        )

    @patch.object(GlobalConfiguration, "mount_container")
    @patch.object(GlobalConfiguration, "create_tenant_config")
    @patch.object(GlobalConfiguration, "product_config")
    @patch.object(Authorisation, "refresh_mounts")
    def test_init_not_reconfigure(
        self,
        mocked_refresh_mounts,
        mocked_product_config,
        mocked_create_tenant_config,
        mocked_mount_container,
    ):
        # arrange
        tenant_config_path = GlobalConfiguration.TENANT_CONFIG_PATH
        GlobalConfiguration.TENANT_CONFIG_PATH = self.tenant_config

        # act
        config = GlobalConfiguration(reconfigure=False)

        GlobalConfiguration.TENANT_CONFIG_PATH = tenant_config_path

        # assert
        mocked_mount_container.assert_has_calls(
            [call("config", "/mnt/config", False), call("logs", "/mnt/logs", False)]
        )
        mocked_create_tenant_config.assert_called_once()
        mocked_product_config.assert_called_once()
        mocked_refresh_mounts.assert_not_called()
        self.assertEqual(
            config.tenant_product_df.count(), 2, msg="config was not correctly read."
        )

    @patch.object(GlobalConfiguration, "mount_container")
    @patch.object(GlobalConfiguration, "create_tenant_config")
    @patch.object(GlobalConfiguration, "product_config")
    @patch.object(Authorisation, "refresh_mounts")
    def test_init_reconfigure(
        self,
        mocked_refresh_mounts,
        mocked_product_config,
        mocked_create_tenant_config,
        mocked_mount_container,
    ):
        # arrange
        tenant_config_path = GlobalConfiguration.TENANT_CONFIG_PATH
        GlobalConfiguration.TENANT_CONFIG_PATH = self.tenant_config

        # act
        config = GlobalConfiguration(reconfigure=True)

        GlobalConfiguration.TENANT_CONFIG_PATH = tenant_config_path

        # assert
        mocked_mount_container.assert_has_calls(
            [call("config", "/mnt/config", True), call("logs", "/mnt/logs", True)]
        )
        mocked_create_tenant_config.assert_called_once()
        mocked_product_config.assert_called_once()
        mocked_refresh_mounts.assert_called_once()
        self.assertEqual(
            config.tenant_product_df.count(), 2, msg="config was not correctly read."
        )

    def test_get_product_configuration_carelink(self):

        product_config = GlobalConfiguration._get_product_configuration(
            "tenant", "carelink", self.database_folder
        )
        self.assertEqual(
            type(CarelinkConfiguration("tenant", self.database_folder)),
            type(product_config),
            msg="Wrong configuration was chosen for carelink product.",
        )

    def test_get_product_configuration_unknown(self):

        with self.assertRaises(
            ProductConfigurationNotFoundException,
            msg="Different exception than ProductConfigurationNotFoundException is not expected.",
        ) as cm:
            GlobalConfiguration._get_product_configuration(
                "tenant", "unknown_product", self.database_folder
            )

    def test_get_tenant_mount_point(self):
        result = GlobalConfiguration.get_tenant_mount_point("tenant1")
        self.assertEqual("/mnt/tenant1", result, msg="Tenant mounting point is wrong.")

    def test_create_tenant_config(self):
        # arrange
        tenant_config_path = GlobalConfiguration.TENANT_CONFIG_PATH
        GlobalConfiguration.TENANT_CONFIG_PATH = self.tenant_config
        env_tenant_config_path = GlobalConfiguration.ENV_TENANT_CONFIG_PATH
        new_env_config_path = f"{self.test_folder}/config/env_config/tenant_config"
        GlobalConfiguration.ENV_TENANT_CONFIG_PATH = new_env_config_path

        # act
        GlobalConfiguration.create_tenant_config()

        GlobalConfiguration.TENANT_CONFIG_PATH = tenant_config_path
        GlobalConfiguration.ENV_TENANT_CONFIG_PATH = env_tenant_config_path

        # assert
        file_content = self.dbutils.fs.head(new_env_config_path)
        self.assertEqual(
            '{"tenant1": ["carelink"], "tenant3": ["carelink"]}',
            file_content,
            msg="env config is incorrect.",
        )

    def test_get_job_config(self):
        # arrange
        global_jobs_config_path = GlobalConfiguration.GLOBAL_JOBS_CONFIG_PATH
        tenant_config_path = GlobalConfiguration.TENANT_CONFIG_PATH

        GlobalConfiguration.TENANT_CONFIG_PATH = self.tenant_config
        GlobalConfiguration.GLOBAL_JOBS_CONFIG_PATH = self.global_config

        # act
        result = GlobalConfiguration.get_job_config(self.active_tenant)

        GlobalConfiguration.TENANT_CONFIG_PATH = tenant_config_path
        GlobalConfiguration.GLOBAL_JOBS_CONFIG_PATH = global_jobs_config_path

        # assert
        expected_config = {
            "NotificationDestinations": [],
            "NotificationRecipients": ["piotr.kwiatkowski@totalmobile.co.uk"],
            "JobDurationThreshold": 600,
            "JobTimeoutThreshold": 1200,
            "JobRetentionPeriodInDays": 3,
            "ClusterName": "main_runner",
            "TasksParallelStreams": 8,
        }
        self.assertDictEqual(expected_config, result, msg="Job config is incorrect.")

    def test_get_tenant_job_retention_period(self):
        # arrange
        global_jobs_config_path = GlobalConfiguration.GLOBAL_JOBS_CONFIG_PATH
        tenant_config_path = GlobalConfiguration.TENANT_CONFIG_PATH

        GlobalConfiguration.TENANT_CONFIG_PATH = self.tenant_config
        GlobalConfiguration.GLOBAL_JOBS_CONFIG_PATH = self.global_config

        # act
        result = GlobalConfiguration.get_tenant_job_retention_period()

        GlobalConfiguration.TENANT_CONFIG_PATH = tenant_config_path
        GlobalConfiguration.GLOBAL_JOBS_CONFIG_PATH = global_jobs_config_path

        # assert
        expected_config = {"tenant1": 3, "tenant2": 5, "tenant3": 5}
        self.assertDictEqual(
            expected_config, result, msg="Job job retention periods are incorrect."
        )

    def test_product_config(self):

        product_config_folder_path = GlobalConfiguration.PRODUCT_CONFIG_FOLDER_PATH
        env_product_config_path = GlobalConfiguration.ENV_PRODUCT_CONFIG_PATH

        GlobalConfiguration.PRODUCT_CONFIG_FOLDER_PATH = (
            f"{self.test_folder}/config/product_config/"
        )
        GlobalConfiguration.ENV_PRODUCT_CONFIG_PATH = (
            f"{self.test_folder}/config/env_config/product_config.json"
        )

        product_config = [
            {
                "SchemaName": "dbo",
                "TableName": "Address",
                "HistoryTableName": "AddressHistory",
                "HistoryTableSchemaName": "Audit",
                "PrimaryKeyColumn": "Id",
                "LoadType": "Incremental",
                "TableType": "SystemVersioned",
                "IncrementalColumn": "ValidFrom",
                "HistoryIncrementalColumn": "ValidTo",
            },
            {
                "SchemaName": "dbo",
                "TableName": "VisitTime",
                "HistoryTableName": None,
                "PrimaryKeyColumn": None,
                "LoadType": "Full",
                "TableType": "Reference",
                "IncrementalColumn": None,
                "HistoryIncrementalColumn": None,
            },
            {
                "SchemaName": "auth",
                "TableName": "EventLog",
                "HistoryTableName": None,
                "PrimaryKeyColumn": None,
                "LoadType": "Full",
                "TableType": "HotReference",
                "IncrementalColumn": None,
                "HistoryIncrementalColumn": None,
            },
        ]
        self.dbutils.fs.put(
            f"{GlobalConfiguration.PRODUCT_CONFIG_FOLDER_PATH}/carelink.json",
            json.dumps(product_config),
            overwrite=True,
        )

        GlobalConfiguration.product_config()
        file_content = self.dbutils.fs.head(GlobalConfiguration.ENV_PRODUCT_CONFIG_PATH)

        GlobalConfiguration.PRODUCT_CONFIG_FOLDER_PATH = product_config_folder_path
        GlobalConfiguration.ENV_PRODUCT_CONFIG_PATH = env_product_config_path

        # assert
        self.assertEqual(
            '{"carelink": ["dbo|Address|LIVE|Id|ValidFrom", "Audit|AddressHistory|HISTORY|Id|ValidFrom", "dbo|VisitTime|REFERENCE||", "auth|EventLog|REFERENCE||"]}',
            file_content,
            msg="env product config is incorrect.",
        )

    @patch.object(
        GlobalConfiguration, "get_tenant_mount_point", return_value="mounted_point"
    )
    @patch.object(GlobalConfiguration, "mount_container")
    @patch.object(GlobalConfiguration, "init_db")
    @patch.object(GlobalConfiguration, "create_tenant_config")
    @patch.object(GlobalConfiguration, "product_config")
    def test_configure_tenant(
        self,
        mocked_product_config,
        mocked_create_tenant_config,
        mocked_init_db,
        mocked_mount_container,
        mocked_get_tenant_mount_point,
    ):
        # arrange
        tenant_config_path = GlobalConfiguration.TENANT_CONFIG_PATH
        GlobalConfiguration.TENANT_CONFIG_PATH = self.tenant_config

        # act
        GlobalConfiguration().configure_tenant(self.active_tenant)
        GlobalConfiguration.TENANT_CONFIG_PATH = tenant_config_path
        # assert
        mocked_get_tenant_mount_point.assert_called_once_with(self.active_tenant)
        mocked_mount_container.assert_has_calls(
            [
                call("config", "/mnt/config", False),
                call("logs", "/mnt/logs", False),
                call(self.active_tenant, "mounted_point"),
            ]
        )
        mocked_init_db.assert_called_once_with(
            self.active_tenant, "mounted_point", self.database_folder
        )

    @patch.object(GlobalConfiguration, "mount_container")
    @patch.object(GlobalConfiguration, "create_tenant_config")
    @patch.object(GlobalConfiguration, "product_config")
    def test_init_db(
        self, mocked_product_config, mocked_create_tenant_config, mocked_mount_container
    ):
        # arrange
        tenant_config_path = GlobalConfiguration.TENANT_CONFIG_PATH
        GlobalConfiguration.TENANT_CONFIG_PATH = self.tenant_config
        config = GlobalConfiguration()
        mocked_spark = MagicMock()
        config.spark = mocked_spark

        # act
        config.init_db(
            self.active_tenant, f"/mnt/{self.active_tenant}", self.database_folder
        )
        GlobalConfiguration.TENANT_CONFIG_PATH = tenant_config_path

        # assert
        mocked_spark.assert_has_calls(
            [
                call.sql(
                    f"CREATE DATABASE IF NOT EXISTS {self.active_tenant} LOCATION '/mnt/{self.active_tenant}/{self.database_folder}'"
                )
            ]
        )

    @patch.object(GlobalConfiguration, "mount_container")
    @patch.object(GlobalConfiguration, "create_tenant_config")
    @patch.object(GlobalConfiguration, "product_config")
    @patch.object(CarelinkConfiguration, "init_bronze_db")
    #@patch.object(DatamartConfiguration, "init_bronze_db")
    @patch.object(AuditLogger, "init_db")
    def test_configure_product(
        self,
        mocekd_audit_init,
        mocekd_carelink_config,
        mocked_product_config,
        mocked_create_tenant_config,
        mocked_mount_container,
    ):
        # arrange
        tenant_config_path = GlobalConfiguration.TENANT_CONFIG_PATH
        GlobalConfiguration.TENANT_CONFIG_PATH = self.tenant_config

        config = GlobalConfiguration()

        # act
        config.configure_product("tenant_name", "carelink")
        GlobalConfiguration.TENANT_CONFIG_PATH = tenant_config_path

        # assert
        mocekd_carelink_config.assert_called_once()
        mocekd_audit_init.assert_called_once_with(
            f"/mnt/tenant_name/{self.database_folder}"
        )

    @patch.object(GlobalConfiguration, "create_tenant_config")
    @patch.object(GlobalConfiguration, "product_config")
    @patch.object(Authorisation, "get_container_client")
    @patch.object(Authorisation, "mount")
    def test_mount_container(
        self,
        mocked_mount,
        mocekd_get_container_client,
        mocked_product_config,
        mocked_create_tenant_config,
    ):
        # arrange
        tenant_config_path = GlobalConfiguration.TENANT_CONFIG_PATH
        GlobalConfiguration.TENANT_CONFIG_PATH = self.tenant_config

        config = GlobalConfiguration()
        container_name = "container_name"
        mount_point = "mount_point"
        mocked_instance = mocekd_get_container_client.return_value
        mocked_instance.exists.return_value = False

        # act
        config.mount_container(container_name, mount_point)
        GlobalConfiguration.TENANT_CONFIG_PATH = tenant_config_path

        # assert
        mocekd_get_container_client.assert_has_calls(
            [
                call("config"),
                call().exists(),
                call().exists().__bool__(),
                call("logs"),
                call().exists(),
                call().exists().__bool__(),
                call("container_name"),
                call().exists(),
                call().create_container(),
            ]
        )
        mocked_mount.assert_has_calls(
            [
                call("config", "/mnt/config", False),
                call("logs", "/mnt/logs", False),
                call("container_name", "mount_point", False),
            ]
        )

    @patch.object(GlobalConfiguration, "mount_container")
    @patch.object(GlobalConfiguration, "create_tenant_config")
    @patch.object(GlobalConfiguration, "product_config")
    @patch.object(GlobalConfiguration, "configure_tenant")
    @patch.object(GlobalConfiguration, "configure_product")
    def test_init_tenants_unknown_tenant(
        self,
        mocked_configure_product,
        mocked_configure_tenant,
        mocked_product_config,
        mocked_create_tenant_config,
        mocked_mount_container,
    ):
        # arrange
        tenant_config_path = GlobalConfiguration.TENANT_CONFIG_PATH
        GlobalConfiguration.TENANT_CONFIG_PATH = self.tenant_config
        config = GlobalConfiguration(reconfigure=False)

        # act
        config.init_tenants("unknown")
        GlobalConfiguration.TENANT_CONFIG_PATH = tenant_config_path

        # assert
        mocked_configure_product.assert_not_called()
        mocked_configure_tenant.assert_not_called()

    @patch.object(GlobalConfiguration, "mount_container")
    @patch.object(GlobalConfiguration, "create_tenant_config")
    @patch.object(GlobalConfiguration, "product_config")
    @patch.object(GlobalConfiguration, "configure_tenant")
    @patch.object(GlobalConfiguration, "configure_product")
    def test_init_tenants_tenant1(
        self,
        mocked_configure_product,
        mocked_configure_tenant,
        mocked_product_config,
        mocked_create_tenant_config,
        mocked_mount_container,
    ):
        # arrange
        tenant_config_path = GlobalConfiguration.TENANT_CONFIG_PATH
        GlobalConfiguration.TENANT_CONFIG_PATH = self.tenant_config
        config = GlobalConfiguration(reconfigure=False)

        # act
        config.init_tenants(self.active_tenant)
        GlobalConfiguration.TENANT_CONFIG_PATH = tenant_config_path

        # assert
        mocked_configure_product.assert_called_once_with(self.active_tenant, ANY)
        mocked_configure_tenant.assert_called_once_with(self.active_tenant)

    @patch.object(GlobalConfiguration, "mount_container")
    @patch.object(GlobalConfiguration, "create_tenant_config")
    @patch.object(GlobalConfiguration, "product_config")
    @patch.object(GlobalConfiguration, "configure_tenant")
    @patch.object(GlobalConfiguration, "configure_product")
    def test_init_tenants_all(
        self,
        mocked_configure_product,
        mocked_configure_tenant,
        mocked_product_config,
        mocked_create_tenant_config,
        mocked_mount_container,
    ):
        # arrange
        tenant_config_path = GlobalConfiguration.TENANT_CONFIG_PATH
        GlobalConfiguration.TENANT_CONFIG_PATH = self.tenant_config
        config = GlobalConfiguration(reconfigure=False)

        # act
        config.init_tenants()
        GlobalConfiguration.TENANT_CONFIG_PATH = tenant_config_path

        # assert
        mocked_configure_product.assert_has_calls(
            [call(self.active_tenant, ANY), call(self.active_tenant2, ANY)]
        )
        mocked_configure_tenant.assert_has_calls(
            [call(self.active_tenant), call(self.active_tenant2)]
        )

    @classmethod
    def tearDownClass(cls):
        print("Testing TestConfiguration - tearDownClass...")

        cls.dbutils.fs.rm(cls.test_folder, True)
        print("Testing TestConfiguration - tearDownClass... DONE")


class TestCarelinkConfiguration(TestCase):

    @classmethod
    def setUpClass(cls):
        print("Testing TestCarelinkConfiguration - setUpClass...")

        cls.test_folder = "/TestCarelinkConfiguration"
        cls.bronze_db_schema = (
            f"{cls.test_folder}/product_config/carelink_bronze_schema.txt"
        )
        cls.spark = SparkSession.builder.getOrCreate()
        cls.dbutils = DBUtils(cls.spark)
        cls.database_folder = "lakehouse"
        ddl = """CREATE TABLE IF NOT EXISTS {table_prefix}_dbo_referralvisit (
                    tenant STRING
                 )
                 USING DELTA
                 LOCATION '{bronze_location}/{table_prefix}_dbo_referralvisit';
                 CREATE TABLE IF NOT EXISTS {table_prefix}_dbo_address (
                    Id STRING
                 )
                 USING DELTA
                 LOCATION '{bronze_location}/{table_prefix}_dbo_address'"""

        cls.dbutils.fs.put(cls.bronze_db_schema, ddl, overwrite=True)

    def test_init_bronze_db(self):
        # arrange
        config_path = GlobalConfiguration.CONFIG_PATH
        GlobalConfiguration.CONFIG_PATH = self.test_folder
        db_folder = self.database_folder
        carelink_config = CarelinkConfiguration("tenant1", db_folder)
        mocked_spark = MagicMock()
        carelink_config.spark = mocked_spark

        # act
        carelink_config.init_bronze_db()
        GlobalConfiguration.CONFIG_PATH = config_path

        # assert
        mocked_spark.assert_has_calls(
            [
                call.sql(
                    f"""CREATE TABLE IF NOT EXISTS tenant1.bronze_carelink_dbo_referralvisit (
                    tenant STRING
                 )
                 USING DELTA
                 LOCATION '/mnt/tenant1/{db_folder}/bronze/tenant1.bronze_carelink_dbo_referralvisit'"""
                ),
                call.sql(
                    f"""
                 CREATE TABLE IF NOT EXISTS tenant1.bronze_carelink_dbo_address (
                    Id STRING
                 )
                 USING DELTA
                 LOCATION '/mnt/tenant1/{db_folder}/bronze/tenant1.bronze_carelink_dbo_address'"""
                ),
            ]
        )

    @classmethod
    def tearDownClass(cls):
        print("Testing TestCarelinkConfiguration - setUpClass...")
        cls.dbutils.fs.rm(cls.test_folder, True)
        print("Testing TestCarelinkConfiguration - tearDownClass... DONE")


class TestLiveSchedulerConfiguration(TestCase):

    @classmethod
    def setUpClass(cls):
        print("Testing TestLiveSchedulerConfiguration - setUpClass...")

        cls.test_folder = "/TestLiveSchedulerConfiguration"
        cls.bronze_db_schema = (
            f"{cls.test_folder}/product_config/scheduler_bronze_schema.txt"
        )
        cls.spark = SparkSession.builder.getOrCreate()
        cls.dbutils = DBUtils(cls.spark)
        cls.database_folder = "lakehouse"
        ddl = """CREATE TABLE IF NOT EXISTS {table_prefix}_dbo_RM_RESOURCE (
                    RES_GUID	STRING,
                    RES_USER_NAME	STRING,
                    RES_FORENAME	STRING,
                    RES_SURNAME	STRING,
                    RES_PHONE_HOME	STRING,
                    RES_PHONE_MOBILE	STRING,
                    RES_PHONE_WORK	STRING,
                    RES_EMAIL	STRING,
                    RES_JOB_TITLE	STRING,
                    RES_ADDRESS	STRING,
                    RES_GEOX	DOUBLE,
                    RES_GEOY	DOUBLE,
                    RES_USER_KEY	STRING,
                    RES_USR_ID	INT,
                    RES_ACTIVE	STRING,
                    RES_WORKINPROGRESS	INT,
                    RES_TRAVEL_MODE	STRING,
                    RES_RANGE_MAX	DOUBLE,
                    RES_ORIGIN	STRING,
                    RES_PICTURE_FILE_ID	STRING,
                    RES_WEEK_WORK_HOURS	DOUBLE,
                    RES_BANDING_GROUP	STRING,
                    RES_INDEFINITE_LEAVE	STRING,
                    RES_PREFERENCE_GROUP	STRING,
                    RES_MIN_VALUE_THRESHOLD	INT,
                    RES_EFFICIENCY	INT
                )
                USING DELTA
                LOCATION '{bronze_location}/{table_prefix}_dbo_RM_RESOURCE';

                CREATE TABLE IF NOT EXISTS {table_prefix}_dbo_RM_RESOURCE_ATTRIBUTE (
                    RAT_GUID STRING
                )
                USING DELTA
                LOCATION '{bronze_location}/{table_prefix}_dbo_RM_RESOURCE_ATTRIBUTE';"""

        cls.dbutils.fs.put(cls.bronze_db_schema, ddl, overwrite=True)

    def test_init_bronze_db(self):
        # arrange
        config_path = GlobalConfiguration.CONFIG_PATH
        GlobalConfiguration.CONFIG_PATH = self.test_folder
        db_folder = self.database_folder
        carelink_config = LiveSchedulerConfiguration("schedulertenant", db_folder)
        mocked_spark = MagicMock()
        carelink_config.spark = mocked_spark

        # act
        carelink_config.init_bronze_db()
        GlobalConfiguration.CONFIG_PATH = config_path

        # assert
        mocked_spark.assert_has_calls(
            [
                call.sql(
                    f"""CREATE TABLE IF NOT EXISTS schedulertenant.bronze_scheduler_dbo_RM_RESOURCE (
                    RES_GUID	STRING,
                    RES_USER_NAME	STRING,
                    RES_FORENAME	STRING,
                    RES_SURNAME	STRING,
                    RES_PHONE_HOME	STRING,
                    RES_PHONE_MOBILE	STRING,
                    RES_PHONE_WORK	STRING,
                    RES_EMAIL	STRING,
                    RES_JOB_TITLE	STRING,
                    RES_ADDRESS	STRING,
                    RES_GEOX	DOUBLE,
                    RES_GEOY	DOUBLE,
                    RES_USER_KEY	STRING,
                    RES_USR_ID	INT,
                    RES_ACTIVE	STRING,
                    RES_WORKINPROGRESS	INT,
                    RES_TRAVEL_MODE	STRING,
                    RES_RANGE_MAX	DOUBLE,
                    RES_ORIGIN	STRING,
                    RES_PICTURE_FILE_ID	STRING,
                    RES_WEEK_WORK_HOURS	DOUBLE,
                    RES_BANDING_GROUP	STRING,
                    RES_INDEFINITE_LEAVE	STRING,
                    RES_PREFERENCE_GROUP	STRING,
                    RES_MIN_VALUE_THRESHOLD	INT,
                    RES_EFFICIENCY	INT
                )
                USING DELTA
                LOCATION '/mnt/schedulertenant/{db_folder}/bronze/schedulertenant.bronze_scheduler_dbo_RM_RESOURCE'"""
                ),
                call.sql(
                    f"""

                CREATE TABLE IF NOT EXISTS schedulertenant.bronze_scheduler_dbo_RM_RESOURCE_ATTRIBUTE (
                    RAT_GUID STRING
                )
                USING DELTA
                LOCATION '/mnt/schedulertenant/lakehouse/bronze/schedulertenant.bronze_scheduler_dbo_RM_RESOURCE_ATTRIBUTE'"""
                ),
            ]
        )

    @classmethod
    def tearDownClass(cls):
        cls.dbutils.fs.rm(cls.test_folder, True)
        print("Testing TestLiveSchedulerConfiguration - tearDownClass... DONE")


class TestTotalMobileConfiguration(TestCase):

    @classmethod
    def setUpClass(cls):
        print("Testing TestTotalMobileConfiguration - setUpClass...")

        cls.test_folder = "/TestTotalMobileConfiguration"
        cls.bronze_db_schema = (
            f"{cls.test_folder}/product_config/totalmobile_bronze_schema.txt"
        )
        cls.spark = SparkSession.builder.getOrCreate()
        cls.dbutils = DBUtils(cls.spark)
        cls.database_folder = "lakehouse"
        ddl = """CREATE TABLE IF NOT EXISTS {table_prefix}_auth_EventLog (
                    Id LONG,
                    Timestamp TIMESTAMP,
                    Name STRING,
                    Category STRING,
                    Type STRING,
                    IpAddress STRING,
                    Body STRING
                )
                USING DELTA
                LOCATION '{bronze_location}/{table_prefix}_auth_EventLog';

                CREATE TABLE IF NOT EXISTS {table_prefix}_dbo_TM_DEVICE (
                    DEV_ID	INT,
                    DEV_DEVICE_SID	STRING,
                    DEV_DEVICE_ID	STRING,
                    DEV_OS	STRING,
                    DEV_PROCESSOR	STRING,
                    DEV_LAST_IP	STRING,
                    DEV_ACTIVE	STRING
                )
                USING DELTA
                LOCATION '{bronze_location}/{table_prefix}_dbo_TM_DEVICE';"""

        cls.dbutils.fs.put(cls.bronze_db_schema, ddl, overwrite=True)

    def test_init_bronze_db(self):
        # arrange
        config_path = GlobalConfiguration.CONFIG_PATH
        GlobalConfiguration.CONFIG_PATH = self.test_folder
        db_folder = self.database_folder
        carelink_config = TotalMobileConfiguration("totalmobiletenant", db_folder)
        mocked_spark = MagicMock()
        carelink_config.spark = mocked_spark

        # act
        carelink_config.init_bronze_db()
        GlobalConfiguration.CONFIG_PATH = config_path

        # assert
        mocked_spark.assert_has_calls(
            [
                call.sql(
                    f"""CREATE TABLE IF NOT EXISTS totalmobiletenant.bronze_totalmobile_auth_EventLog (
                    Id LONG,
                    Timestamp TIMESTAMP,
                    Name STRING,
                    Category STRING,
                    Type STRING,
                    IpAddress STRING,
                    Body STRING
                )
                USING DELTA
                LOCATION '/mnt/totalmobiletenant/{db_folder}/bronze/totalmobiletenant.bronze_totalmobile_auth_EventLog'"""
                ),
                call.sql(
                    f"""

                CREATE TABLE IF NOT EXISTS totalmobiletenant.bronze_totalmobile_dbo_TM_DEVICE (
                    DEV_ID	INT,
                    DEV_DEVICE_SID	STRING,
                    DEV_DEVICE_ID	STRING,
                    DEV_OS	STRING,
                    DEV_PROCESSOR	STRING,
                    DEV_LAST_IP	STRING,
                    DEV_ACTIVE	STRING
                )
                USING DELTA
                LOCATION '/mnt/totalmobiletenant/{db_folder}/bronze/totalmobiletenant.bronze_totalmobile_dbo_TM_DEVICE'"""
                ),
            ]
        )

    @classmethod
    def tearDownClass(cls):
        cls.dbutils.fs.rm(cls.test_folder, True)
        print("Testing TestTotalMobileConfiguration - tearDownClass... DONE")


class TestDatamartConfiguration(TestCase):

    @classmethod
    def setUpClass(cls):
        print("Testing TestDatamartConfiguration - setUpClass...")

        cls.test_folder = "/TestDatamartConfiguration"
        cls.bronze_db_schema = (
            f"{cls.test_folder}/product_config/datamart_bronze_schema.txt"
        )
        cls.spark = SparkSession.builder.getOrCreate()
        cls.dbutils = DBUtils(cls.spark)
        cls.database_folder = "lakehouse"
        ddl = """CREATE TABLE IF NOT EXISTS {table_prefix}_dbo_ResourceAvailability(
                    Type STRING, 
                    ResourceUsername STRING, 
                    Start TIMESTAMP, 
                    End TIMESTAMP, 
                    Description STRING, 
                    Reference STRING, 
                    Reason STRING, 
                    Deleted STRING, 
                    WorldReference STRING, 
                    SourceWorldReference STRING, 
                    DeletionReason STRING, 
                    DeletionNotes STRING, 
                    DeletedByUserName STRING, 
                    Address STRING, 
                    Geox FLOAT, 
                    Geoy FLOAT, 
                    ClearOverlappingJobs STRING 
                )
                USING DELTA
                LOCATION '{bronze_location}/{table_prefix}_dbo_ResourceAvailability';

                CREATE TABLE IF NOT EXISTS {table_prefix}_dbo_Resources(
                    ResourceKey STRING, 
                    UserName STRING, 
                    ForeName STRING, 
                    SurName STRING, 
                    HomePhone STRING, 
                    MobilePhone STRING, 
                    WorkPhone STRING, 
                    Email STRING, 
                    JobTitle STRING, 
                    Address STRING, 
                    Geox FLOAT, 
                    Geoy FLOAT, 
                    UserKey STRING, 
                    UserId INT, 
                    TravelMode STRING, 
                    Range FLOAT, 
                    WorkInProgress INT, 
                    WeekWorkHours FLOAT, 
                    BandingGroup STRING, 
                    IndefiniteLeave STRING, 
                    PreferenceGroup STRING, 
                    MinValueThreshold INT, 
                    Efficiency INT 
                )
                USING DELTA
                LOCATION '{bronze_location}/{table_prefix}_dbo_Resources';"""

        cls.dbutils.fs.put(cls.bronze_db_schema, ddl, overwrite=True)

    def test_init_bronze_db(self):
        # arrange
        config_path = GlobalConfiguration.CONFIG_PATH
        GlobalConfiguration.CONFIG_PATH = self.test_folder
        db_folder = self.database_folder
        datamart_config = DatamartConfiguration("datamarttenant", db_folder)
        mocked_spark = MagicMock()
        datamart_config.spark = mocked_spark

        # act
        datamart_config.init_bronze_db()
        GlobalConfiguration.CONFIG_PATH = config_path

        # assert
        mocked_spark.assert_has_calls(
            [
                call.sql(
                    f"""CREATE TABLE IF NOT EXISTS datamarttenant.bronze_datamart_dbo_ResourceAvailability(
                    Type STRING, 
                    ResourceUsername STRING, 
                    Start TIMESTAMP, 
                    End TIMESTAMP, 
                    Description STRING, 
                    Reference STRING, 
                    Reason STRING, 
                    Deleted STRING, 
                    WorldReference STRING, 
                    SourceWorldReference STRING, 
                    DeletionReason STRING, 
                    DeletionNotes STRING, 
                    DeletedByUserName STRING, 
                    Address STRING, 
                    Geox FLOAT, 
                    Geoy FLOAT, 
                    ClearOverlappingJobs STRING 
                )
                USING DELTA
                LOCATION '/mnt/datamarttenant/{db_folder}/bronze/datamarttenant.bronze_datamart_dbo_ResourceAvailability'"""
                ),
                call.sql(
                    f"""

                CREATE TABLE IF NOT EXISTS datamarttenant.bronze_datamart_dbo_Resources(
                    ResourceKey STRING, 
                    UserName STRING, 
                    ForeName STRING, 
                    SurName STRING, 
                    HomePhone STRING, 
                    MobilePhone STRING, 
                    WorkPhone STRING, 
                    Email STRING, 
                    JobTitle STRING, 
                    Address STRING, 
                    Geox FLOAT, 
                    Geoy FLOAT, 
                    UserKey STRING, 
                    UserId INT, 
                    TravelMode STRING, 
                    Range FLOAT, 
                    WorkInProgress INT, 
                    WeekWorkHours FLOAT, 
                    BandingGroup STRING, 
                    IndefiniteLeave STRING, 
                    PreferenceGroup STRING, 
                    MinValueThreshold INT, 
                    Efficiency INT 
                )
                USING DELTA
                LOCATION '/mnt/datamarttenant/{db_folder}/bronze/datamarttenant.bronze_datamart_dbo_Resources'"""
                ),
            ]
        )

    @classmethod
    def tearDownClass(cls):
        cls.dbutils.fs.rm(cls.test_folder, True)
        print("Testing TestDatamartConfiguration - tearDownClass... DONE")