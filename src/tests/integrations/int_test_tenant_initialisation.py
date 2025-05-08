import unittest
import json
import os
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from modules.configuration import GlobalConfiguration
from modules.exceptions import UnityExecutionException
from modules.authorisation import Authorisation

class TestTenantInitialization(unittest.TestCase):

    integration_mount_config = "/mnt/integrationconfig"
    integration_config_container = "integrationconfig"
    integration_mount_point = '/mnt/integration'
    tenant_name = "successfultenant"
    failing_tenant_name = "failingtenant"
    inactive_tenant_name = "inactivetenant"
    
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
        spark = SparkSession.getActiveSession()
        return DBUtils(spark)

    @classmethod
    def setUpClass(cls):
        print("Testing tenant initialization script... Setup")

        cls.spark = SparkSession.getActiveSession()

        ########### PREPARE TENANT_PRODUCT.JSON CONFIG ##################################################
        print("Creating sample tenant_product.json and putting the file into dbfs:/")

        integration_config = [
            {
                "TenantName": cls.tenant_name,
                "ProductName": "carelink",
                "IsActive": True
            },
            {
                "TenantName": cls.tenant_name,
                "ProductName": "totalmobile",
                "IsActive": True
            },
            {
                "TenantName": cls.tenant_name,
                "ProductName": "scheduler",
                "IsActive": True
            },
            {
                "TenantName": cls.failing_tenant_name,
                "ProductName": "carelink", 
                "IsActive": True
            },
            {
                "TenantName": cls.tenant_name,
                "ProductName": "datamart",
                "IsActive": True
            },
            {
                "TenantName": cls.inactive_tenant_name,
                "ProductName": "mobilise",
                "IsActive": False
            },
            {
                "TenantName": cls.tenant_name,
                "ProductName": "unknownproduct",
                "IsActive": True
            }
        ]

        client = Authorisation.get_container_client(cls.integration_config_container)
        if not client.exists():
            client.create_container()
        
        Authorisation.mount(cls.integration_config_container, cls.integration_mount_config, True)

        tenant_dbfs_location = f"{cls.integration_mount_config}/tenant_config/tenant_product.json"  

        dbutils = cls.get_dbutils()


        print("tenant_dbfs_location", tenant_dbfs_location)
        dbutils.fs.put(
            tenant_dbfs_location, json.dumps(integration_config), overwrite=True
        )

        ########### COPY PRODUCT(CARELINK).JSON CONFIG ##################################################
        print("Copying carelink.json config into dbfs:/")

        product_cfg_src_path = "/mnt/config/product_config/carelink.json"
        product_cfg_dest_path = f"{cls.integration_mount_config}/product_config/carelink.json" 

        dbutils.fs.cp(product_cfg_src_path, product_cfg_dest_path, True)

        ########### COPY DDL_SCHEMA.TXT CONFIG ##########################################################
        print("Copying ddl.txt config into dbfs:/")

        ddl_cfg_src_path = "/mnt/config/product_config/carelink_bronze_schema.txt"
        ddl_cfg_dest_path = f"{cls.integration_mount_config}/product_config/carelink_bronze_schema.txt" 
        
        dbutils.fs.cp(ddl_cfg_src_path, ddl_cfg_dest_path, True)
        
        ddl_cfg_src_path = "/mnt/config/product_config/totalmobile_bronze_schema.txt"
        ddl_cfg_dest_path = f"{cls.integration_mount_config}/product_config/totalmobile_bronze_schema.txt" 
        
        dbutils.fs.cp(ddl_cfg_src_path, ddl_cfg_dest_path, True)
        
        ddl_cfg_src_path = "/mnt/config/product_config/scheduler_bronze_schema.txt"
        ddl_cfg_dest_path = f"{cls.integration_mount_config}/product_config/scheduler_bronze_schema.txt" 
        
        dbutils.fs.cp(ddl_cfg_src_path, ddl_cfg_dest_path, True)
        
        ddl_cfg_src_path = "/mnt/config/product_config/datamart_bronze_schema.txt"
        ddl_cfg_dest_path = f"{cls.integration_mount_config}/product_config/datamart_bronze_schema.txt" 
        
        dbutils.fs.cp(ddl_cfg_src_path, ddl_cfg_dest_path, True)

        test_data = [("Sunset Boulevard", 28), ("Cedar Street", 35), ("High Avenue", 22)]
        test_columns = ["StreetName", "HouseNo"]

        client = Authorisation.get_container_client(cls.failing_tenant_name)
        if not client.exists():
            client.create_container()
        
        Authorisation.mount(cls.failing_tenant_name, f"/mnt/{cls.failing_tenant_name}", True)

        test_table_location = f"/mnt/{cls.failing_tenant_name}/lakehouse/bronze/{cls.failing_tenant_name}.bronze_carelink_dbo_address"
        failing_df = cls.spark.createDataFrame(test_data, test_columns)
        failing_df.write.option("mergeSchema", "true").mode("overwrite").save(test_table_location)

        Authorisation.unmount(cls.integration_mount_config)

    def test_init(self):
        dbutils = self.get_dbutils()

        self.TMP_MOUNT_PREFIX = GlobalConfiguration.MOUNT_PREFIX
        self.TMP_CONFIG_PATH = GlobalConfiguration.CONFIG_PATH
        self.TMP_CONFIG_CONTAINER = GlobalConfiguration.CONFIG_CONTAINER
        self.TMP_ENV_CONFIG_FOLDER_PATH = GlobalConfiguration.ENV_CONFIG_FOLDER_PATH
        self.TMP_GLOBAL_CONFIG_FOLDER_PATH = GlobalConfiguration.GLOBAL_CONFIG_FOLDER_PATH
        self.TMP_GLOBAL_JOBS_CONFIG_PATH = GlobalConfiguration.GLOBAL_JOBS_CONFIG_PATH
        self.TMP_ENV_TENANT_CONFIG_PATH = GlobalConfiguration.ENV_TENANT_CONFIG_PATH
        self.TMP_ENV_PRODUCT_CONFIG_PATH = GlobalConfiguration.ENV_PRODUCT_CONFIG_PATH
        self.TMP_PRODUCT_CONFIG_FOLDER_PATH = GlobalConfiguration.PRODUCT_CONFIG_FOLDER_PATH
        self.TMP_TENANT_CONFIG_PATH = GlobalConfiguration.TENANT_CONFIG_PATH 
        GlobalConfiguration.MOUNT_PREFIX = self.integration_mount_config
        GlobalConfiguration.CONFIG_CONTAINER = self.integration_config_container
        GlobalConfiguration.CONFIG_PATH = f"{GlobalConfiguration.MOUNT_PREFIX}/config"
        GlobalConfiguration.ENV_CONFIG_FOLDER_PATH = f"{GlobalConfiguration.CONFIG_PATH}/env_config"
        GlobalConfiguration.PRODUCT_CONFIG_FOLDER_PATH = f"{GlobalConfiguration.CONFIG_PATH}/product_config"
        GlobalConfiguration.GLOBAL_CONFIG_FOLDER_PATH = f"{GlobalConfiguration.CONFIG_PATH}/global_config"
        GlobalConfiguration.TENANT_CONFIG_PATH = f"{GlobalConfiguration.CONFIG_PATH}/tenant_config/tenant_product.json"
        GlobalConfiguration.GLOBAL_JOBS_CONFIG_PATH = f"{GlobalConfiguration.GLOBAL_CONFIG_FOLDER_PATH}/jobs.json"
        GlobalConfiguration.ENV_TENANT_CONFIG_PATH = f"{GlobalConfiguration.ENV_CONFIG_FOLDER_PATH}/tenant_config.json"
        GlobalConfiguration.ENV_PRODUCT_CONFIG_PATH = f"{GlobalConfiguration.ENV_CONFIG_FOLDER_PATH}/product_config.json"

        expected_table_count = 107

        # act
        config = GlobalConfiguration(True)
        with self.assertRaises(UnityExecutionException) as init_tenants_context:
            config.init_tenants()

        ################ CHECK IF EXCEPTION FOR UNKNOWNPRODUCT HAS BEEN THROWN ############################
        self.assertIn("Exception: Unknown Product found: unknownproduct!", str(init_tenants_context.exception))

        ################ CHECK IF EXCEPTION INVALID_DDL_SCHEMA HAS BEEN THROWN ############################
        self.assertIn("The specified schema does not match the existing schema", str(init_tenants_context.exception))

        ################ CHECK IF DATABASE EXISTS ##########################################################
        databases = self.spark.sql("SHOW DATABASES").collect()
        database_names = [row["databaseName"] for row in databases]

        self.assertIn(self.tenant_name, database_names, f"Database {self.tenant_name} not found.")

        ################ CHECK IF TABLE COUNT BETWEEN DATABASE AND DDL CONFIG MATCHES #######################
        self.spark.sql(f"USE {self.tenant_name}")
        actual_table_count = self.spark.sql("SHOW TABLES").count()
        self.assertEqual(expected_table_count, actual_table_count, f"Table count mismatch in database {self.tenant_name}.")

        ################ CHECK IF PRODUCT_CONFIG HAS BEEN CREATED ###########################################
        dbfs_file_path = f"/dbfs{self.integration_mount_config}/config/env_config/product_config.json"
        path_exists = os.path.exists(dbfs_file_path)

        self.assertTrue(path_exists, f"The file '{dbfs_file_path}' does not exist.")

        ################ CHECK IF TENANT_CONFIG HAS BEEN CREATED #############################################
        dbfs_file_path = f"/dbfs{self.integration_mount_config}/config/env_config/tenant_config.json"
        path_exists = os.path.exists(dbfs_file_path)

        self.assertTrue(path_exists, f"The file '{dbfs_file_path}' does not exist.")

        with open(dbfs_file_path, 'r') as file:
            content = file.read()

        json_config = json.loads(content)
        integration_tenant_exists = json_config.get(self.tenant_name)
        inactive_tenant_not_exists = json_config.get(self.inactive_tenant_name)

        self.assertTrue(integration_tenant_exists, f"The active tenant '{self.tenant_name}' does not exist in the env config.")
        self.assertFalse(inactive_tenant_not_exists, f"The inactive tenant '{self.inactive_tenant_name}' does exist in the env config.")

    @classmethod
    def tearDownClass(cls):
        dbutils = cls.get_dbutils()

        print(f"Dropping sample schemas.")
        cls.spark.sql(f"DROP DATABASE IF EXISTS {cls.tenant_name} CASCADE")
        cls.spark.sql(f"DROP DATABASE IF EXISTS {cls.failing_tenant_name} CASCADE")
        cls.spark.sql(f"DROP DATABASE IF EXISTS {cls.inactive_tenant_name} CASCADE")

        print(f"Dropping all {cls.integration_mount_point} sample directories.")
        dbutils.fs.rm(cls.integration_mount_config, True)

        print("Unmounting all tenant initialisation points")
        dbutils.fs.refreshMounts()
        all_mounts = dbutils.fs.mounts()
        mount_point = f"{cls.integration_mount_config}/config"
        if any(mount.mountPoint == mount_point for mount in all_mounts):
            Authorisation.unmount(mount_point)
        mount_point = f"{cls.integration_mount_config}/{cls.tenant_name}"
        if any(mount.mountPoint == mount_point for mount in all_mounts):
            Authorisation.unmount(mount_point)
        mount_point = f"/mnt/{cls.failing_tenant_name}"
        if any(mount.mountPoint == mount_point for mount in all_mounts):
            Authorisation.unmount(mount_point)
        mount_point = f"{cls.integration_mount_config}/{cls.failing_tenant_name}"
        if any(mount.mountPoint == mount_point for mount in all_mounts):
            Authorisation.unmount(mount_point)
        
        Authorisation.get_container_client(cls.integration_config_container).delete_container()
        Authorisation.get_container_client(cls.tenant_name).delete_container()
        Authorisation.get_container_client(cls.failing_tenant_name).delete_container()
        
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
