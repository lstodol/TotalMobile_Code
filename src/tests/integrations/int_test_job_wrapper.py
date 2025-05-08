import unittest, json
from datetime import datetime, timedelta
from modules.authorisation import Authorisation
from modules.configuration import GlobalConfiguration
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

from modules.configuration_utils import ConfigurationUtils
from modules.exceptions import UnityExecutionException
from modules.job_manager import JobManager

class TestJobWrapperIntegration(unittest.TestCase):

    job_wrapper_mount_point = '/mnt/jobwrapper'
    config_container = "jobwrapper"
    db_folder = "lakehouse"
    app_layer = "bronze"
    product = "carelink"
    schema = "dbo"
    succeeded_tenant = "tenant1"
    failed_tenant = "tenant2"
    exception_tenant = "tenant3"
    
    #Temporary variables for config paths
    TMP_MOUNT_PREFIX = ''
    TMP_CONFIG_PATH = ''
    TMP_CONFIG_CONTAINER = ''
    TMP_ENV_CONFIG_FOLDER_PATH = ''
    TMP_GLOBAL_CONFIG_FOLDER_PATH = ''
    TMP_GLOBAL_JOBS_CONFIG_PATH = ''
    TMP_ENV_TENANT_CONFIG_PATH = ''
    TMP_ENV_PRODUCT_CONFIG_PATH = ''
    TMP_PRODUCT_CONFIG_FOLDER_PATH = ''
    TMP_TENANT_CONFIG_PATH = ''

    @classmethod
    def get_dbutils(cls):
        cls.spark = SparkSession.getActiveSession()
        return DBUtils(cls.spark)

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.getActiveSession()
        cls.dbutils = cls.get_dbutils()

        ########### PREPARE TENANT_PRODUCT.JSON CONFIG ####################################################
        print("Creating sample tenant_product.json and putting it into dbfs:/")

        tenant_test_config = [
            {
                "TenantName": cls.succeeded_tenant,
                "ProductName": cls.product,
                "IsActive": True
            },
            {
                "TenantName": cls.failed_tenant,
                "ProductName": cls.product,
                "IsActive": True
            },
            {
                "TenantName": cls.exception_tenant,
                "ProductName": cls.product,
                "IsActive": True
            }
        ]
        tenant_config_test_json = json.dumps(tenant_test_config)
        tenant_config_test_location = f"{cls.job_wrapper_mount_point}/tenant_config/tenant_product.json"  

        cls.dbutils.fs.put(tenant_config_test_location, tenant_config_test_json, overwrite=True)

        ############ PREPARE PRODUCT(CARELINK).JSON CONFIG ##################################################
        print("Preparing carelink.json config and putting it into dbfs:/")

        carelink_json = [
            {"SchemaName": "dbo", "TableName": "Address", "HistoryTableName": "AddressHistory", "HistoryTableSchemaName": "Audit", "PrimaryKeyColumn": "Id", "LoadType": "Incremental", "TableType": "SystemVersioned", "IncrementalColumn": "ValidFrom", "HistoryIncrementalColumn": "ValidTo"},
            {"SchemaName": "dbo", "TableName": "ServiceUser", "HistoryTableName": "ServiceUserHistory", "HistoryTableSchemaName": "Audit", "PrimaryKeyColumn": "Id", "LoadType": "Incremental", "TableType": "SystemVersioned", "IncrementalColumn": "ValidFrom", "HistoryIncrementalColumn": "ValidTo"},
            {"SchemaName": "dbo", "TableName": "ServiceUserAddress", "HistoryTableName": "ServiceUserAddressHistory", "HistoryTableSchemaName": "Audit", "PrimaryKeyColumn": "Id", "LoadType": "Incremental", "TableType": "SystemVersioned", "IncrementalColumn": "ValidFrom", "HistoryIncrementalColumn": "ValidTo"},
            {"SchemaName": "dbo", "TableName": "ServiceUserUnavailability", "HistoryTableName": "ServiceUserUnavailabilityHistory", "HistoryTableSchemaName": "Audit", "PrimaryKeyColumn": "Id", "LoadType": "Incremental", "TableType": "SystemVersioned", "IncrementalColumn": "ValidFrom", "HistoryIncrementalColumn": "ValidTo"},
            {"SchemaName": "dbo", "TableName": "VisitTime", "HistoryTableName": None, "PrimaryKeyColumn": None, "LoadType": "Full", "TableType": "Reference", "IncrementalColumn": None, "HistoryIncrementalColumn": None}
            ]

        carelink_test_config_json = json.dumps(carelink_json)
        carelink_test_config_json_location = f"{cls.job_wrapper_mount_point}/product_config/{cls.product}.json"  

        cls.dbutils.fs.put(carelink_test_config_json_location, carelink_test_config_json, overwrite=True)

        ########### PREPARE DDL_SCHEMA.TXT CONFIG ##########################################################
        print("Preparing carelink ddl config and putting it into dbfs:/")

        ddl_schema = """CREATE TABLE IF NOT EXISTS {table_prefix}_dbo_address (
                Id STRING,
                Name STRING,
                AddressLine1 STRING,
                AddressLine2 STRING,
                AddressLine3 STRING,
                AddressLine4 STRING,
                AddressLine5 STRING,
                Postcode STRING,
                IsDefault BOOLEAN,
                Lat DOUBLE,
                Long DOUBLE,
                LastModifiedByClientId STRING,
                LastModifiedByUserId STRING,
                LastModifiedByUsername STRING,
                ValidFrom TIMESTAMP,
                ValidTo TIMESTAMP,
                AddressType STRING,
                TemporaryEndDate TIMESTAMP,
                TemporaryEndTimeId STRING,
                TemporaryStartDate TIMESTAMP,
                TemporaryStartTimeId STRING,
                AddressGroupId STRING,
                LoadDate TIMESTAMP
            )
            USING DELTA
            LOCATION '{bronze_location}/{table_prefix}_dbo_address';

            CREATE TABLE IF NOT EXISTS {history_table_prefix}_audit_addresshistory (
                Id STRING,
                Name STRING,
                AddressLine1 STRING,
                AddressLine2 STRING,
                AddressLine3 STRING,
                AddressLine4 STRING,
                AddressLine5 STRING,
                Postcode STRING,
                IsDefault BOOLEAN,
                Lat DOUBLE,
                Long DOUBLE,
                LastModifiedByClientId STRING,
                LastModifiedByUserId STRING,
                LastModifiedByUsername STRING,
                ValidFrom TIMESTAMP,
                ValidTo TIMESTAMP,
                AddressType STRING,
                TemporaryEndDate TIMESTAMP,
                TemporaryEndTimeId STRING,
                TemporaryStartDate TIMESTAMP,
                TemporaryStartTimeId STRING,
                AddressGroupId STRING,
                LoadDate TIMESTAMP
            )
            USING DELTA
            LOCATION '{bronze_location}/{history_table_prefix}_audit_addresshistory';

            CREATE TABLE IF NOT EXISTS {table_prefix}_dbo_serviceuser (
                Id STRING,
                NhsNumber STRING,
                ExternalReference STRING,
                Title STRING,
                Forename STRING,
                Surname STRING,
                PreferredName STRING,
                DateOfBirth TIMESTAMP,
                DateOfDeath TIMESTAMP,
                Phone1 STRING,
                Phone2 STRING,
                Phone3 STRING,
                Email STRING,
                Gender STRING,
                Ethnicity STRING,
                Nationality STRING,
                MaritalStatus STRING,
                Religion STRING,
                FirstLanguage STRING,
                LivesAlone BOOLEAN,
                Interpreter BOOLEAN,
                PractitionerId STRING,
                IsNHSTemporary BOOLEAN,
                CreatedDate TIMESTAMP,
                LastModifiedDate TIMESTAMP,
                Reference INT,
                LastModifiedByClientId STRING,
                LastModifiedByUserId STRING,
                LastModifiedByUsername STRING,
                ValidFrom TIMESTAMP,
                ValidTo TIMESTAMP,
                ExternalId STRING,
                ExternalAppId STRING,
                NearFieldCommunication STRING,
                LoadDate TIMESTAMP
            )
            USING DELTA
            LOCATION '{bronze_location}/{table_prefix}_dbo_serviceuser';

            CREATE TABLE IF NOT EXISTS {history_table_prefix}_audit_serviceuserhistory (
                Id STRING,
                NhsNumber STRING,
                ExternalReference STRING,
                Title STRING,
                Forename STRING,
                Surname STRING,
                PreferredName STRING,
                DateOfBirth TIMESTAMP,
                DateOfDeath TIMESTAMP,
                Phone1 STRING,
                Phone2 STRING,
                Phone3 STRING,
                Email STRING,
                Gender STRING,
                Ethnicity STRING,
                Nationality STRING,
                MaritalStatus STRING,
                Religion STRING,
                FirstLanguage STRING,
                LivesAlone BOOLEAN,
                Interpreter BOOLEAN,
                PractitionerId STRING,
                IsNHSTemporary BOOLEAN,
                CreatedDate TIMESTAMP,
                LastModifiedDate TIMESTAMP,
                Reference INT,
                LastModifiedByClientId STRING,
                LastModifiedByUserId STRING,
                LastModifiedByUsername STRING,
                ValidFrom TIMESTAMP,
                ValidTo TIMESTAMP,
                ExternalId STRING,
                ExternalAppId STRING,
                NearFieldCommunication STRING,
                LoadDate TIMESTAMP
            )
            USING DELTA
            LOCATION '{bronze_location}/{history_table_prefix}_audit_serviceuserhistory';

            CREATE TABLE IF NOT EXISTS {table_prefix}_dbo_serviceuseraddress (
                Id STRING,
                ServiceUserId STRING,
                AddressId STRING,
                ValidFrom TIMESTAMP,
                ValidTo TIMESTAMP,
                LoadDate TIMESTAMP
            )
            USING DELTA
            LOCATION '{bronze_location}/{table_prefix}_dbo_serviceuseraddress';

            CREATE TABLE IF NOT EXISTS {history_table_prefix}_audit_serviceuseraddresshistory (
                Id STRING,
                ServiceUserId STRING,
                AddressId STRING,
                ValidFrom TIMESTAMP,
                ValidTo TIMESTAMP,
                LoadDate TIMESTAMP
            )
            USING DELTA
            LOCATION '{bronze_location}/{history_table_prefix}_audit_serviceuseraddresshistory';

            CREATE TABLE IF NOT EXISTS {table_prefix}_dbo_serviceuserunavailability (
                Id STRING,
                ServiceUserId STRING,
                StartDate TIMESTAMP,
                StartTimeId STRING,
                EndDate TIMESTAMP,
                EndTimeId STRING,
                Notes STRING,
                Reason STRING,
                LastModifiedByClientId STRING,
                LastModifiedByUserId STRING,
                LastModifiedByUsername STRING,
                ValidFrom TIMESTAMP,
                ValidTo TIMESTAMP,
                LoadDate TIMESTAMP
            ) 
            USING DELTA
            LOCATION '{bronze_location}/{table_prefix}_dbo_serviceuserunavailability';

            CREATE TABLE IF NOT EXISTS {history_table_prefix}_audit_serviceuserunavailabilityhistory (
                Id STRING,
                ServiceUserId STRING,
                StartDate TIMESTAMP,
                StartTimeId STRING,
                EndDate TIMESTAMP,
                EndTimeId STRING,
                Notes STRING,
                Reason STRING,
                LastModifiedByClientId STRING,
                LastModifiedByUserId STRING,
                LastModifiedByUsername STRING,
                ValidFrom TIMESTAMP,
                ValidTo TIMESTAMP,
                LoadDate TIMESTAMP
            ) 
            USING DELTA
            LOCATION '{bronze_location}/{history_table_prefix}_audit_serviceuserunavailabilityhistory';

            CREATE TABLE IF NOT EXISTS {table_prefix}_dbo_visittime (
                Id STRING,
                Time STRING,
                Order INT,
                LoadDate TIMESTAMP
            )
            USING DELTA
            LOCATION '{bronze_location}/{table_prefix}_dbo_visittime'
            """

        ddl_schema_location = f"{cls.job_wrapper_mount_point}/product_config/{cls.product}_{cls.app_layer}_schema.txt"

        cls.dbutils.fs.put(ddl_schema_location, ddl_schema, overwrite=True)
        cls.mock_config_path()

        config = GlobalConfiguration(True)
        cls.dbutils.fs.cp(f"{cls.job_wrapper_mount_point}/product_config/", f"{cls.job_wrapper_mount_point}/config/product_config/", recurse=True)

        ########### INIT TENANTS + INSERT RECORD INTO JOB AUDIT ################################
        print("Initializing all tenants and inserting a sample record into the audit table")
        GlobalConfiguration.MOUNT_PREFIX = cls.job_wrapper_mount_point
        
        config.init_tenants()
        
        dts = str(datetime.now())
        str_dts = datetime.now()
        future_dts = str_dts + timedelta(hours=2)

        table_schema = [
            "tenant", "product", "job_name", "job_id", "job_run_id", "job_start_time", "job_end_time",
            "job_run_page_url", "number_of_tasks", "job_status", "job_error_desc", "updated_by", "updated_at"
        ]
        successful_row = [(cls.succeeded_tenant, cls.product, f"bronze_{cls.succeeded_tenant}_{cls.product}_{future_dts}", 1, 1, dts, dts, "n/a", 1, "SUCCESS", "n/a", cls.succeeded_tenant, str_dts)]
        successful_row_table = f"{cls.succeeded_tenant}.{cls.app_layer}_{cls.product}_job_audit"
        successful_row_df = cls.spark.createDataFrame(successful_row, table_schema)
        successfull_path = f"{cls.job_wrapper_mount_point}/{cls.succeeded_tenant}/{cls.db_folder}/{cls.app_layer}/{cls.succeeded_tenant}.{cls.app_layer}_{cls.product}_job_audit"
        successful_row_df.write.format("delta").option("path", successfull_path).mode("overwrite").saveAsTable(successful_row_table)

        failure_row = [(cls.failed_tenant, cls.product, f"bronze_{cls.failed_tenant}_{cls.product}_{future_dts}", 1, 1, dts, dts, "n/a", 1, "RUNNING", "n/a", cls.failed_tenant, str_dts)]
        failure_row_df = cls.spark.createDataFrame(failure_row, table_schema)
        failure_row_table = f"{cls.failed_tenant}.{cls.app_layer}_{cls.product}_job_audit"
        failure_path = f"{cls.job_wrapper_mount_point}/{cls.failed_tenant}/{cls.db_folder}/{cls.app_layer}/{cls.failed_tenant}.{cls.app_layer}_{cls.product}_job_audit"
        failure_row_df.write.format("delta").option("path", failure_path).mode("overwrite").saveAsTable(failure_row_table)

        #break the exception tenant by removing audit table 
        cls.dbutils.fs.rm(f"{cls.job_wrapper_mount_point}/{cls.exception_tenant}/{cls.db_folder}/{cls.app_layer}/{cls.exception_tenant}.{cls.app_layer}_{cls.product}_job_audit", True)

        ########### COPY NEW ENV_TENANT CONFIG ################################
        print("Copying new environment tenant config.")

        cls.new_env_tenant_config = {"tenant1": ["carelink"], "tenant2": ["carelink"], "tenant3": ["carelink"]}
        cls.dbutils.fs.put(f"{cls.job_wrapper_mount_point}/config/env_config/tenant_config.json", json.dumps(cls.new_env_tenant_config), overwrite=True)

        cls.global_config = {
            "NotificationDestinations": [],
            "NotificationRecipients": ["anonymous@totalmobile.co.uk"],
            "JobDurationThreshold": 300,
            "JobTimeoutThreshold": 600,
            "JobRetentionPeriodInDays": 7,
            "ClusterName": "main_runner",
            "TasksParallelStreams": 10
        }

        cls.dbutils.fs.put(GlobalConfiguration.GLOBAL_JOBS_CONFIG_PATH, json.dumps(cls.global_config), overwrite=True)
       
        cls.product_dict = ConfigurationUtils.read_config(GlobalConfiguration.ENV_PRODUCT_CONFIG_PATH)

        ########### PREPARE PATH FOR SAMPLE NOTEBOOK ########################################################
        print("Preparing path for sample notebook.")

        sample_notebook = "test_sample_notebook"
        cls.context = json.loads(cls.dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
        cls.current_path = cls.context['extraContext']['notebook_path'].rsplit('/', 1)[0]
        cls.sample_notebook_path = f"{cls.current_path}/{sample_notebook}"

    @classmethod
    def mock_config_path(cls):
        cls.TMP_MOUNT_PREFIX = GlobalConfiguration.MOUNT_PREFIX
        cls.TMP_CONFIG_PATH = GlobalConfiguration.CONFIG_PATH
        cls.TMP_CONFIG_CONTAINER = GlobalConfiguration.CONFIG_CONTAINER
        cls.TMP_ENV_CONFIG_FOLDER_PATH = GlobalConfiguration.ENV_CONFIG_FOLDER_PATH
        cls.TMP_GLOBAL_CONFIG_FOLDER_PATH = GlobalConfiguration.GLOBAL_CONFIG_FOLDER_PATH
        cls.TMP_GLOBAL_JOBS_CONFIG_PATH = GlobalConfiguration.GLOBAL_JOBS_CONFIG_PATH
        cls.TMP_ENV_TENANT_CONFIG_PATH = GlobalConfiguration.ENV_TENANT_CONFIG_PATH
        cls.TMP_ENV_PRODUCT_CONFIG_PATH = GlobalConfiguration.ENV_PRODUCT_CONFIG_PATH
        cls.TMP_PRODUCT_CONFIG_FOLDER_PATH = GlobalConfiguration.PRODUCT_CONFIG_FOLDER_PATH
        cls.TMP_TENANT_CONFIG_PATH = GlobalConfiguration.TENANT_CONFIG_PATH 
        GlobalConfiguration.MOUNT_PREFIX = cls.job_wrapper_mount_point
        GlobalConfiguration.CONFIG_PATH = f"{GlobalConfiguration.MOUNT_PREFIX}/config"
        GlobalConfiguration.CONFIG_CONTAINER = cls.config_container
        GlobalConfiguration.ENV_CONFIG_FOLDER_PATH = f"{GlobalConfiguration.CONFIG_PATH}/env_config"
        GlobalConfiguration.GLOBAL_CONFIG_FOLDER_PATH = f"{GlobalConfiguration.CONFIG_PATH}/global_config"
        GlobalConfiguration.GLOBAL_JOBS_CONFIG_PATH = f"{GlobalConfiguration.GLOBAL_CONFIG_FOLDER_PATH}/jobs.json"
        GlobalConfiguration.ENV_TENANT_CONFIG_PATH = f"{GlobalConfiguration.ENV_CONFIG_FOLDER_PATH}/tenant_config.json"
        GlobalConfiguration.ENV_PRODUCT_CONFIG_PATH = f"{GlobalConfiguration.ENV_CONFIG_FOLDER_PATH}/product_config.json"
        GlobalConfiguration.PRODUCT_CONFIG_FOLDER_PATH = f"{cls.job_wrapper_mount_point}/product_config"
        GlobalConfiguration.TENANT_CONFIG_PATH = f"{cls.job_wrapper_mount_point}/tenant_config/tenant_product.json"
        
    def test_run_job_manager(self):
        cluster_name = self.global_config["ClusterName"]
        tenant_dict = self.new_env_tenant_config
        product_dict = self.product_dict
        layer = self.app_layer
        script_path = self.sample_notebook_path

        with self.assertRaises(UnityExecutionException) as context:
            job_manager = JobManager(cluster_name=cluster_name, tenant_dict=tenant_dict, product_dict=product_dict, layer=layer, script_path=script_path)
            job_manager.create_and_run_job()

        print("Performing checks...")
        
        self.assertIn("Exception: [DELTA_TABLE_NOT_FOUND] Delta table `tenant3`.`bronze_carelink_job_audit` doesn't exist.", str(context.exception))

        succeeded_query = f"SELECT COUNT(*) FROM {self.succeeded_tenant}.{self.app_layer}_{self.product}_job_audit"
        succeeded_result = self.spark.sql(succeeded_query).collect()[0][0]
        self.assertEqual(succeeded_result, 2, "Task and job have not been created")

        failed_query = f"SELECT COUNT(*) FROM {self.failed_tenant}.{self.app_layer}_{self.product}_job_audit"
        failed_result = self.spark.sql(failed_query).collect()[0][0]
        self.assertEqual(failed_result, 1, "Task and job creation executed despite latest state is RUNNING.")

    @classmethod
    def tearDownClass(cls):
        dbutils = cls.get_dbutils()

        print(f"Dropping sample schemas.") 
        cls.spark.sql(f"DROP DATABASE IF EXISTS {cls.succeeded_tenant} CASCADE")
        cls.spark.sql(f"DROP DATABASE IF EXISTS {cls.failed_tenant} CASCADE")
        cls.spark.sql(f"DROP DATABASE IF EXISTS {cls.exception_tenant} CASCADE")

        print(f"Dropping all {cls.job_wrapper_mount_point} sample directories.")
        dbutils.fs.rm(cls.job_wrapper_mount_point, True)
        
        print(f"Unmounting all {cls.job_wrapper_mount_point} directories.")
        cls.dbutils.fs.refreshMounts()
        all_mounts = dbutils.fs.mounts()
        
        mount_point = f"{cls.job_wrapper_mount_point}/{cls.succeeded_tenant}"
        if any(mount.mountPoint == mount_point for mount in all_mounts):
            Authorisation.unmount(mount_point)
        mount_point = f"{cls.job_wrapper_mount_point}/{cls.failed_tenant}"
        if any(mount.mountPoint == mount_point for mount in all_mounts):
            Authorisation.unmount(mount_point)
        mount_point = f"{cls.job_wrapper_mount_point}/{cls.exception_tenant}"
        if any(mount.mountPoint == mount_point for mount in all_mounts):
            Authorisation.unmount(mount_point)
        mount_point = f"{cls.job_wrapper_mount_point}/config"
        if any(mount.mountPoint == mount_point for mount in all_mounts):
            Authorisation.unmount(mount_point)
        
        print(f"Dropping all sample containers.")
        Authorisation.get_container_client(cls.succeeded_tenant).delete_container()
        Authorisation.get_container_client(cls.failed_tenant).delete_container()
        Authorisation.get_container_client(cls.config_container).delete_container()
        Authorisation.get_container_client(cls.exception_tenant).delete_container()
        
        print(f"Restoring original GlobalConfiguration paths.")
        GlobalConfiguration.MOUNT_PREFIX = cls.TMP_MOUNT_PREFIX
        GlobalConfiguration.CONFIG_PATH = cls.TMP_CONFIG_PATH
        GlobalConfiguration.CONFIG_CONTAINER = cls.TMP_CONFIG_CONTAINER
        GlobalConfiguration.ENV_CONFIG_FOLDER_PATH = cls.TMP_ENV_CONFIG_FOLDER_PATH
        GlobalConfiguration.GLOBAL_CONFIG_FOLDER_PATH = cls.TMP_GLOBAL_CONFIG_FOLDER_PATH
        GlobalConfiguration.GLOBAL_JOBS_CONFIG_PATH = cls.TMP_GLOBAL_JOBS_CONFIG_PATH
        GlobalConfiguration.ENV_TENANT_CONFIG_PATH = cls.TMP_ENV_TENANT_CONFIG_PATH
        GlobalConfiguration.ENV_PRODUCT_CONFIG_PATH = cls.TMP_ENV_PRODUCT_CONFIG_PATH
        GlobalConfiguration.PRODUCT_CONFIG_FOLDER_PATH = cls.TMP_PRODUCT_CONFIG_FOLDER_PATH
        GlobalConfiguration.TENANT_CONFIG_PATH  = cls.TMP_TENANT_CONFIG_PATH
