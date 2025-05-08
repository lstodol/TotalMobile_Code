import unittest
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from modules.configuration import GlobalConfiguration
from modules.bronze_layer import IngestBronzeTable
from modules.authorisation import Authorisation


class TestLoadBronzeTable(unittest.TestCase):
    config_mount_location = "/mnt/inttestloadbronzetableconfig"
    config_container = "inttestloadbronzetableconfig"
    tenant = "inttestloadbronzetabletenant"
    product_carelink = "carelink"
    product_datamart = "datamart"

    @classmethod
    def get_dbutils(cls):
        cls.spark = SparkSession.getActiveSession()
        return DBUtils(cls.spark)

    @classmethod
    def create_tenant_product_config(cls):
        integration_test_config = [
            {"TenantName": cls.tenant, "ProductName": "carelink", "IsActive": True}
            , {"TenantName": cls.tenant, "ProductName": "datamart", "IsActive": True}
        ]

        config_location = (
            f"{cls.config_mount_location}/tenant_config/tenant_product.json"
        )

        cls.dbutils.fs.put(
            config_location, json.dumps(integration_test_config), overwrite=True
        )

    @classmethod
    def create_product_config_ddl(cls):
        carelink_ddl = """
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
            
            CREATE TABLE IF NOT EXISTS {table_prefix}_dbo_visittime (
                Id STRING,
                Time STRING,
                Order INT,
                LoadDate TIMESTAMP
            )
            USING DELTA
            LOCATION '{bronze_location}/{table_prefix}_dbo_visittime'
            """

        carelink_bronze_db_schema_location = f"{cls.config_mount_location}/config/product_config/carelink_bronze_schema.txt"

        cls.dbutils.fs.put(carelink_bronze_db_schema_location, carelink_ddl, overwrite=True)
        
        datamart_ddl = """
            CREATE TABLE IF NOT EXISTS {table_prefix}_dbo_visitadditionalproperties( 
                PropertyKey STRING, 
                VisitParentKey STRING, 
                Name STRING, 
                Value STRING, 
                PropertyParentKey STRING, 
                LastUpdatedUtc TIMESTAMP,
                ADFRunId STRING,
                LoadDate TIMESTAMP 
            ) 
            USING DELTA 
            LOCATION '{bronze_location}/{table_prefix}_dbo_visitadditionalproperties';
            
            CREATE TABLE IF NOT EXISTS {table_prefix}_dbo_rosters( 
                Reference STRING, 
                Name STRING, 
                Deleted STRING, 
                RosterKey STRING, 
                DeletedByUserName STRING, 
                DeletedDate TIMESTAMP, 
                LastUpdatedUtc TIMESTAMP,
                ADFRunId STRING,
                LoadDate TIMESTAMP 
            ) 
            USING DELTA 
            LOCATION '{bronze_location}/{table_prefix}_dbo_rosters'
            """

        datamart_bronze_db_schema_location = f"{cls.config_mount_location}/config/product_config/datamart_bronze_schema.txt"

        cls.dbutils.fs.put(datamart_bronze_db_schema_location, datamart_ddl, overwrite=True)

    @classmethod
    def create_product_config(cls):
        carelink_product = [
            {
                "SchemaName": "dbo",
                "TableName": "ServiceUserAddress",
                "HistoryTableName": "ServiceUserAddressHistory",
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
        ]

        carelink_product_json_location = (
            f"{cls.config_mount_location}/product_config/carelink.json"
        )

        cls.dbutils.fs.put(
            carelink_product_json_location, json.dumps(carelink_product), overwrite=True
        )
        
        datamart_product = [
            {
                "SchemaName": "dbo",
                "TableName": "VisitAdditionalProperties",
                "HistoryTableName": None,
                "HistoryTableSchemaName": None,
                "PrimaryKeyColumn": "PropertyKey",
                "LoadType": "Incremental",
                "TableType": "ChangeTrackingVersion",
                "IncrementalColumn": None,
                "HistoryIncrementalColumn": None
            },
            {      
                "SchemaName": "dbo",      
                "TableName": "Rosters",      
                "HistoryTableName": None,      
                "HistoryTableSchemaName": None,      
                "PrimaryKeyColumn": "RosterKey",      
                "LoadType": "Incremental",      
                "TableType": "UpdateTimestamp",      
                "IncrementalColumn": "LastUpdatedUtc",      
                "HistoryIncrementalColumn": None    
        },
        ]

        datamart_product_json_location = (
            f"{cls.config_mount_location}/product_config/datamart.json"
        )

        cls.dbutils.fs.put(
            datamart_product_json_location, json.dumps(datamart_product), overwrite=True
        )

    @classmethod
    def mock_config_path(cls):
        GlobalConfiguration.MOUNT_PREFIX = cls.config_mount_location
        GlobalConfiguration.CONFIG_PATH = f"{GlobalConfiguration.MOUNT_PREFIX}/config"
        GlobalConfiguration.CONFIG_CONTAINER = cls.config_container

        GlobalConfiguration.ENV_CONFIG_FOLDER_PATH = (
            f"{GlobalConfiguration.CONFIG_PATH}/env_config"
        )

        GlobalConfiguration.GLOBAL_CONFIG_FOLDER_PATH = (
            f"{GlobalConfiguration.CONFIG_PATH}/global_config"
        )
        GlobalConfiguration.GLOBAL_JOBS_CONFIG_PATH = (
            f"{GlobalConfiguration.GLOBAL_CONFIG_FOLDER_PATH}/jobs.json"
        )
        GlobalConfiguration.ENV_TENANT_CONFIG_PATH = (
            f"{GlobalConfiguration.ENV_CONFIG_FOLDER_PATH}/tenant_config.json"
        )
        GlobalConfiguration.ENV_PRODUCT_CONFIG_PATH = (
            f"{GlobalConfiguration.ENV_CONFIG_FOLDER_PATH}/product_config.json"
        )

        # test specific location on dbfs
        GlobalConfiguration.PRODUCT_CONFIG_FOLDER_PATH = (
            f"{cls.config_mount_location}/product_config"
        )
        GlobalConfiguration.TENANT_CONFIG_PATH = (
            f"{cls.config_mount_location}/tenant_config/tenant_product.json"
        )

    @classmethod
    def setUpClass(cls):
        print("Testing TestLoadBronzeTable - setUpClass...")
        cls.dbutils = cls.get_dbutils()

        cls.create_tenant_product_config()
        cls.create_product_config()
        cls.mock_config_path()

        config = GlobalConfiguration(False)

        cls.create_product_config_ddl()

        config.init_tenants(cls.tenant)

        print("Testing TestLoadBronzeTable - setUpClass... DONE")

    def test_process_table_reference(self):
        # arrange
        db = self.tenant
        table = "bronze_carelink_dbo_visittime"
        source_table = "visittime"
        tenant_loc = f"/mnt/{self.config_container}/{self.tenant}/"
        source_loc = f"{tenant_loc}/raw/carelink/dbo/visittime"
        schema = "dbo"

        exisitng_data_sql = f"""INSERT INTO {db}.{table} VALUES
                            ("8b7ad264-cb3c-441e-a24d-0073d834c25c", "23:15", 1, "2023-12-08T08:03:05.4360609Z"),
                            ("16e81f68-90f3-4d6a-b2c8-03c798aa8e29", "18:45", 1, "2023-12-08T08:03:05.4360609Z"),
                            ("2d26a3ef-169c-453b-9ce2-056ece18f8cc", "10:00", 1, "2023-12-08T08:03:05.4360609Z");"""

        self.spark.sql(exisitng_data_sql)

        new_data_schema = (
            "Id STRING, Time STRING, Order INT, LoadDate TIMESTAMP, Flag STRING"
        )

        new_data = [
            [
                "6a0d5f61-a87d-4d03-8c7a-08308c9b84fb",
                "13:15",
                1,
                datetime.fromisoformat("2023-12-13 09:23:06.436"),
                "I",
            ],
            [
                "td0d5f61-a23d-4b90-8c7a-08409dfrb84fb",
                "9:30",
                1,
                datetime.fromisoformat("2023-12-13 09:23:06.436"),
                "I",
            ],
            [
                "57804516-1757-485b-b03c-127c96749759",
                "1:15",
                1,
                datetime.fromisoformat("2023-12-13 09:23:06.436"),
                "I",
            ],
        ]

        new_df = self.spark.createDataFrame(new_data, schema=new_data_schema)
        new_df.write.mode("overwrite").parquet(source_loc)
        new_df = new_df.drop("Flag")
        tenant_location = f"/{self.config_container}/{self.tenant}"
        ingestion = IngestBronzeTable(
            self.tenant, "carelink", "dbo", "visittime", "REFERENCE", None, None
        )
        ingestion.raw_file_location = (
            f"/mnt/{tenant_location}/raw/{self.product_carelink}/{schema}/{source_table}"
        )
        ingestion.checkpoint_location = f"/mnt/{tenant_location}/bronze_checkpoint/{self.product_carelink}/{schema}/{source_table}"

        # act
        ingestion.load()

        # assert
        existing_df = self.spark.sql(
            f"SELECT Id, Time, Order, LoadDate FROM {db}.{table}"
        )
        self.assertEqual(
            sorted(new_df.collect()),
            sorted(existing_df.collect()),
            msg="Records mismatch after load of VisitTime reference table.",
        )

    def test_process_table_live(self):
        # arrange
        db = self.tenant
        live_table = "serviceuseraddress"
        table = "bronze_carelink_dbo_serviceuseraddress"
        tenant_loc = f"/mnt/{self.config_container}/{self.tenant}/"
        live_source_loc = f"{tenant_loc}/raw/{self.product_carelink}/dbo/{live_table}"

        # prepare live table
        live_exisitng_data_sql = f"""INSERT INTO {db}.{table} VALUES
                                ("00009805-44E6-4559-5986-08D91477A6A8", "61CEFE78-62CA-4586-F3CC-08D91477A6A7", "CD967965-9483-4A46-71DF-08D91477A6A8", 
                                "2021-05-12T14:11:06.288+0000", "9999-12-31T23:59:59.999+0000", "2023-12-08T07:56:35.153+0000"),
                                ("0004C3A9-B138-4B4A-5C69-08D91477A6A8", "96439497-C8AA-42FA-F6AF-08D91477A6A7", "6699E5B4-3E7B-4D88-74C2-08D91477A6A8", 
                                "2021-05-12T14:36:16.390+0000", "9999-12-31T23:59:59.999+0000", "2023-12-08T07:56:35.153+0000"),
                                ("0025FC32-3687-4519-5817-08D91477A6A8", "A7130186-3603-4395-F25D-08D91477A6A7", "1A96C768-CDAF-433C-7070-08D91477A6A8", 
                                "2021-05-11T15:52:42.810+0000", "9999-12-31T23:59:59.999+0000", "2023-12-08T07:56:35.153+0000")"""

        self.spark.sql(live_exisitng_data_sql)

        new_live_data_schema = "Id STRING, ServiceUserId STRING, AddressId STRING, ValidFrom TIMESTAMP, ValidTo	TIMESTAMP, LoadDate TIMESTAMP, Flag STRING"

        end_valid_to = datetime.fromisoformat("9999-12-31 23:59:59.999")

        new_live_data = [
            [
                "00009805-44E6-4559-5986-08D91477A6A8",
                "61CEFE78-62CA-4586-F3CC-08D91477A6A7",
                "00009805-44E6-4559-5986-08D91477A6A8",
                datetime.fromisoformat("2023-12-13 09:23:06.436"),
                end_valid_to,
                datetime.fromisoformat("2023-12-08 07:56:35.153"),
                "D",
            ],
            [
                "00415737-729B-4F40-5C2A-08D91477A6A8",
                "696DD969-3C48-4FAB-F670-08D91477A6A7",
                "98BADD4A-54EC-4169-7483-08D91477A6A8",
                datetime.fromisoformat("2021-05-12 14:34:16.780"),
                end_valid_to,
                datetime.fromisoformat("2023-12-08 07:56:35.153"),
                "I",
            ],
            [
                "00461423-70CA-4C51-8785-08DA68AAC865",
                "B0B9D6C1-88A2-4553-7381-08DA68AAC864",
                "B45C7A43-E077-418B-CF99-08DA68AAC865",
                datetime.fromisoformat("2020-01-02 18:59:12.340"),
                end_valid_to,
                datetime.fromisoformat("2023-12-08 07:56:35.153"),
                "I",
            ],
        ]

        new_df = self.spark.createDataFrame(new_live_data, schema=new_live_data_schema)
        new_df.write.mode("overwrite").parquet(live_source_loc)

        ingestion = IngestBronzeTable(
            self.tenant, "carelink", "dbo", live_table, "LIVE", "Id, ServiceUserId", "ValidFrom"
        )
        ingestion.raw_file_location = (
            f"{tenant_loc}/raw/{self.product_carelink}/dbo/{live_table}"
        )
        ingestion.checkpoint_location = (
            f"{tenant_loc}/bronze_checkpoint/{self.product_carelink}/dbo/{live_table}"
        )

        # act
        ingestion.load()

        # assert
        existing_df = self.spark.sql(
            f"""SELECT Id, ServiceUserId, AddressId, ValidFrom, ValidTo, LoadDate 
            FROM {db}.{table}"""
        )

        expected_data = [
            [
                "0004C3A9-B138-4B4A-5C69-08D91477A6A8",
                "96439497-C8AA-42FA-F6AF-08D91477A6A7",
                "6699E5B4-3E7B-4D88-74C2-08D91477A6A8",
                datetime.fromisoformat("2021-05-12 14:36:16.390"),
                end_valid_to,
                datetime.fromisoformat("2023-12-08 07:56:35.153"),
            ],
            [
                "0025FC32-3687-4519-5817-08D91477A6A8",
                "A7130186-3603-4395-F25D-08D91477A6A7",
                "1A96C768-CDAF-433C-7070-08D91477A6A8",
                datetime.fromisoformat("2021-05-11 15:52:42.810"),
                end_valid_to,
                datetime.fromisoformat("2023-12-08 07:56:35.153"),
            ],
            [
                "00415737-729B-4F40-5C2A-08D91477A6A8",
                "696DD969-3C48-4FAB-F670-08D91477A6A7",
                "98BADD4A-54EC-4169-7483-08D91477A6A8",
                datetime.fromisoformat("2021-05-12 14:34:16.780"),
                end_valid_to,
                datetime.fromisoformat("2023-12-08 07:56:35.153"),
            ],
            [
                "00461423-70CA-4C51-8785-08DA68AAC865",
                "B0B9D6C1-88A2-4553-7381-08DA68AAC864",
                "B45C7A43-E077-418B-CF99-08DA68AAC865",
                datetime.fromisoformat("2020-01-02 18:59:12.340"),
                end_valid_to,
                datetime.fromisoformat("2023-12-08 07:56:35.153"),
            ],
        ]
        expected_data_schema = "Id STRING, ServiceUserId STRING, AddressId STRING, ValidFrom TIMESTAMP, ValidTo	TIMESTAMP, LoadDate TIMESTAMP"
        expected_df = self.spark.createDataFrame(
            expected_data, schema=expected_data_schema
        )

        self.assertEqual(
            sorted(expected_df.collect()),
            sorted(existing_df.collect()),
            msg="Records mismatch after load of ServiceUserAddress live table.",
        )

    def test_process_table_history(self):
        # arrange
        db = self.tenant
        table = "bronze_carelink_audit_serviceuseraddresshistory"
        history_table = "serviceuseraddresshistory"
        tenant_loc = f"/mnt/{self.config_container}/{self.tenant}/"
        history_source_loc = f"{tenant_loc}/raw/{self.product_carelink}/audit/{history_table}"

        # prepare live table
        live_exisitng_data_sql = f"""INSERT INTO {db}.{table} VALUES
                                ("00009805-44E6-4559-5986-08D91477A6A8", "61CEFE78-62CA-4586-F3CC-08D91477A6A7", "CD967965-9483-4A46-71DF-08D91477A6A8", 
                                "2020-05-12T14:11:06.288+0000", "9999-12-31T23:59:59.999+0000", "2023-12-08T07:56:35.153+0000"),
                                ("0025FC32-3687-4519-5817-08D91477A6A8", "A7130186-3603-4395-F25D-08D91477A6A7", "1A96C768-CDAF-433C-7070-08D91477A6A8", 
                                "2024-05-11T15:52:42.810+0000", "9999-12-31T23:59:59.999+0000", "2023-12-08T07:56:35.153+0000")"""

        self.spark.sql(live_exisitng_data_sql)

        new_live_data_schema = "Id STRING, ServiceUserId STRING, AddressId STRING, ValidFrom TIMESTAMP, ValidTo	TIMESTAMP, LoadDate TIMESTAMP, Flag STRING"

        end_valid_to = datetime.fromisoformat("9999-12-31 23:59:59.999")

        new_live_data = [
            [
                "00009805-44E6-4559-5986-08D91477A6A8",
                "61CEFE78-62CA-4586-F3CC-08D91477A6A7",
                "00009805-44E6-4559-5986-08D91477A6A8",
                datetime.fromisoformat("2023-12-13 09:23:06.436"),
                end_valid_to,
                datetime.fromisoformat("2023-12-08 07:56:35.153"),
                "D",
            ],
            [
                "00415737-729B-4F40-5C2A-08D91477A6A8",
                "696DD969-3C48-4FAB-F670-08D91477A6A7",
                "98BADD4A-54EC-4169-7483-08D91477A6A8",
                datetime.fromisoformat("2021-05-12 14:34:16.780"),
                end_valid_to,
                datetime.fromisoformat("2023-12-08 07:56:35.153"),
                "I",
            ],
        ]

        new_df = self.spark.createDataFrame(new_live_data, schema=new_live_data_schema)
        new_df.write.mode("overwrite").parquet(history_source_loc)

        ingestion = IngestBronzeTable(
            self.tenant, "carelink", "audit", history_table, "HISTORY", "Id, ServiceUserId", "ValidFrom"
        )
        ingestion.raw_file_location = (
            f"{tenant_loc}/raw/{self.product_carelink}/audit/{history_table}"
        )
        ingestion.checkpoint_location = (
            f"{tenant_loc}/bronze_checkpoint/{self.product_carelink}/audit/{history_table}"
        )

        # act
        ingestion.load()

        # assert
        existing_df = self.spark.sql(
            f"""SELECT Id, ServiceUserId, AddressId, ValidFrom, ValidTo, LoadDate 
            FROM {db}.{table}"""
        )

        expected_data = [
            [
                "00009805-44E6-4559-5986-08D91477A6A8",
                "61CEFE78-62CA-4586-F3CC-08D91477A6A7", 
                "CD967965-9483-4A46-71DF-08D91477A6A8", 
                datetime.fromisoformat("2020-05-12 14:11:06.288"),
                end_valid_to, 
                datetime.fromisoformat("2023-12-08 07:56:35.153"),
            ],
            [
                "0025FC32-3687-4519-5817-08D91477A6A8",
                "A7130186-3603-4395-F25D-08D91477A6A7",
                "1A96C768-CDAF-433C-7070-08D91477A6A8",
                datetime.fromisoformat("2024-05-11 15:52:42.810"),
                end_valid_to,
                datetime.fromisoformat("2023-12-08 07:56:35.153"),
            ],
            [
                "00415737-729B-4F40-5C2A-08D91477A6A8",
                "696DD969-3C48-4FAB-F670-08D91477A6A7",
                "98BADD4A-54EC-4169-7483-08D91477A6A8",
                datetime.fromisoformat("2021-05-12 14:34:16.780"),
                end_valid_to,
                datetime.fromisoformat("2023-12-08 07:56:35.153"),
            ],
        ]

        expected_data_schema = "Id STRING, ServiceUserId STRING, AddressId STRING, ValidFrom TIMESTAMP, ValidTo	TIMESTAMP, LoadDate TIMESTAMP"
        expected_df = self.spark.createDataFrame(
            expected_data, schema=expected_data_schema
        )

        self.assertEqual(
            sorted(expected_df.collect()),
            sorted(existing_df.collect()),
            msg="Records mismatch after load of ServiceUserAddressHistory table.",
        )
        
    def test_process_table_ChangeTrackingVersion(self):
        # arrange
        db = self.tenant
        ChangeTrackingVersionTable = "visitadditionalproperties"
        table = "bronze_datamart_dbo_visitadditionalproperties"
        tenant_loc = f"/mnt/{self.config_container}/{self.tenant}/"
        ChangeTrackingVersion_source_loc = f"{tenant_loc}/raw/{self.product_datamart}/dbo/{ChangeTrackingVersionTable}"

        # prepare ChangeTrackingVersion table
        ChangeTrackingVersion_exisitng_data_sql = f"""INSERT INTO {db}.{table} VALUES
                                ("Key1", "SubKey1", "Name1", "Value1", "SubKey1", "2024-05-01T07:56:35.153+0000", "1", "2024-05-01T07:56:35.153+0000"),
                                ("Key2", "SubKey2", "Name2", "Value2", "SubKey2", "2024-05-01T07:56:35.153+0000", "1", "2024-05-01T07:56:35.153+0000"),
                                ("Key3", "SubKey3", "Name3", "Value3", "SubKey3", "2024-05-01T07:56:35.153+0000", "1", "2024-05-01T07:56:35.153+0000")"""

        self.spark.sql(ChangeTrackingVersion_exisitng_data_sql)

        new_ChangeTrackingVersion_data_schema = "PropertyKey STRING, VisitParentKey STRING, Name STRING, Value STRING, PropertyParentKey STRING, LastUpdatedUtc TIMESTAMP, ADFRunId STRING, LoadDate TIMESTAMP, Flag STRING"

        update_ts = datetime.fromisoformat("2024-05-24 00:00:00.000")

        new_ChangeTrackingVersion_data = [
            [
                "Key1", "SubKey1", "Name1", "Value1", "SubKey1",
                update_ts, "2",
                datetime.fromisoformat("2024-05-24 07:56:35.153"),
                "D",
            ],
            [
                "Key3", "SubKey3", "Name3New", "Value3", "SubKey3",
                update_ts, "2",
                datetime.fromisoformat("2024-05-24 07:56:35.153"),
                "IU",
            ],
            [
                "Key5", "SubKey5", "Name5", "Value5", "SubKey5",
                update_ts, "2",
                datetime.fromisoformat("2024-05-24 07:56:35.153"),
                "IU",
            ],
        ]

        new_df = self.spark.createDataFrame(new_ChangeTrackingVersion_data, schema=new_ChangeTrackingVersion_data_schema)
        new_df.write.mode("overwrite").parquet(ChangeTrackingVersion_source_loc)

        ingestion = IngestBronzeTable(
            self.tenant, "datamart", "dbo", ChangeTrackingVersionTable, "CHANGETRACKINGVERSION", "PropertyKey", None
        )
        ingestion.raw_file_location = (
            f"{tenant_loc}/raw/{self.product_datamart}/dbo/{ChangeTrackingVersionTable}"
        )
        ingestion.checkpoint_location = (
            f"{tenant_loc}/bronze_checkpoint/{self.product_datamart}/dbo/{ChangeTrackingVersionTable}"
        )

        # act
        ingestion.load()

        # assert
        existing_df = self.spark.sql(
            f"""SELECT PropertyKey, VisitParentKey, Name, Value, PropertyParentKey, LastUpdatedUtc, ADFRunId, LoadDate 
            FROM {db}.{table} ORDER BY PropertyKey"""
        )

        expected_data = [
            [
                "Key2", "SubKey2", "Name2", "Value2", "SubKey2", datetime.fromisoformat("2024-05-01 07:56:35.153"), "1", datetime.fromisoformat("2024-05-01 07:56:35.153"),
            ],
            [
                "Key3", "SubKey3", "Name3New", "Value3", "SubKey3", 
                update_ts, "2", 
                datetime.fromisoformat("2024-05-24 07:56:35.153"),
            ],
            [
                "Key5", "SubKey5", "Name5", "Value5", "SubKey5",
                update_ts, "2",
                datetime.fromisoformat("2024-05-24 07:56:35.153"),
            ],
        ]
        expected_data_schema = "PropertyKey STRING, VisitParentKey STRING, Name STRING, Value STRING, PropertyParentKey STRING, LastUpdatedUtc TIMESTAMP, ADFRunId STRING, LoadDate TIMESTAMP"
        expected_df = self.spark.createDataFrame(
            expected_data, schema=expected_data_schema
        )

        self.assertEqual(
            sorted(expected_df.collect()),
            sorted(existing_df.collect()),
            msg="Records mismatch after load of VisitAdditionalProperties ChangeTrackingVersion table.",
        )

    def test_process_table_UpdateTimestamp(self):
        # arrange
        db = self.tenant
        UpdateTimestampTable = "rosters"
        table = "bronze_datamart_dbo_rosters"
        tenant_loc = f"/mnt/{self.config_container}/{self.tenant}/"
        UpdateTimestamp_source_loc = f"{tenant_loc}/raw/{self.product_datamart}/dbo/{UpdateTimestampTable}"

        # prepare UpdateTimestamp table
        UpdateTimestamp_exisitng_data_sql = f"""INSERT INTO {db}.{table} VALUES
                                ("Ref1", "Name1", "N", "Key1", "UName1", "2024-05-01T07:56:35.153+0000", "2024-05-01T07:56:35.153+0000", "1", "2024-05-01T07:56:35.153+0000"),
                                ("Ref2", "Name2", "N", "Key2", "UName2", "2024-05-01T07:56:35.153+0000", "2024-05-01T07:56:35.153+0000", "1", "2024-05-01T07:56:35.153+0000"),
                                ("Ref3", "Name3", "N", "Key3", "UName3", "2024-05-01T07:56:35.153+0000", "2024-05-01T07:56:35.153+0000", "1", "2024-05-01T07:56:35.153+0000")"""

        self.spark.sql(UpdateTimestamp_exisitng_data_sql)

        new_UpdateTimestamp_data_schema = "Reference STRING, Name STRING, Deleted STRING, RosterKey STRING, DeletedByUserName STRING, DeletedDate TIMESTAMP, LastUpdatedUtc TIMESTAMP, ADFRunId STRING, LoadDate TIMESTAMP, Flag STRING"

        update_ts = datetime.fromisoformat("2024-01-01 00:00:00.000")

        new_UpdateTimestamp_data = [
            [
                "Ref1", "Name1", "N", "Key1", "UName1", datetime.fromisoformat("2024-05-01 07:56:35.153"),
                update_ts, "2",
                datetime.fromisoformat("2024-05-24 07:56:35.153"),
                "IU",
            ],
            [
                "Ref3", "Name3", "Y", "Key3", "UName3Deleted", datetime.fromisoformat("2024-05-01 07:56:35.153"),
                update_ts, "2",
                datetime.fromisoformat("2024-05-24 07:56:35.153"),
                "IU",
            ],
            [
                "Ref5", "Name5", "N", "Key5", "UName5", datetime.fromisoformat("2024-05-01 07:56:35.153"),
                update_ts, "2",
                datetime.fromisoformat("2024-05-24 07:56:35.153"),
                "IU",
            ],
        ]

        new_df = self.spark.createDataFrame(new_UpdateTimestamp_data, schema=new_UpdateTimestamp_data_schema)
        new_df.write.mode("overwrite").parquet(UpdateTimestamp_source_loc)

        ingestion = IngestBronzeTable(
            self.tenant, "datamart", "dbo", UpdateTimestampTable, "UPDATETIMESTAMP", "RosterKey", "LastUpdatedUtc"
        )
        ingestion.raw_file_location = (
            f"{tenant_loc}/raw/{self.product_datamart}/dbo/{UpdateTimestampTable}"
        )
        ingestion.checkpoint_location = (
            f"{tenant_loc}/bronze_checkpoint/{self.product_datamart}/dbo/{UpdateTimestampTable}"
        )

        # act
        ingestion.load()

        # assert
        existing_df = self.spark.sql(
            f"""SELECT Reference, Name, Deleted, RosterKey, DeletedByUserName, DeletedDate, LastUpdatedUtc, ADFRunId, LoadDate 
            FROM {db}.{table} ORDER BY RosterKey"""
        )

        expected_data = [
            [
                "Ref1", "Name1", "N", "Key1", "UName1", datetime.fromisoformat("2024-05-01 07:56:35.153"),
                update_ts, "2",
                datetime.fromisoformat("2024-05-24 07:56:35.153"),
            ],
            [
                "Ref2", "Name2", "N", "Key2", "UName2", datetime.fromisoformat("2024-05-01 07:56:35.153"), 
                datetime.fromisoformat("2024-05-01 07:56:35.153"), "1", 
                datetime.fromisoformat("2024-05-01 07:56:35.153"),
            ],
            [
                "Ref3", "Name3", "Y", "Key3", "UName3Deleted", datetime.fromisoformat("2024-05-01 07:56:35.153"),
                update_ts, "2",
                datetime.fromisoformat("2024-05-24 07:56:35.153"),
            ],
            [
                "Ref5", "Name5", "N", "Key5", "UName5", datetime.fromisoformat("2024-05-01 07:56:35.153"),
                update_ts, "2",
                datetime.fromisoformat("2024-05-24 07:56:35.153"),
            ],
        ]
        expected_data_schema = "Reference STRING, Name STRING, Deleted STRING, RosterKey STRING, DeletedByUserName STRING, DeletedDate TIMESTAMP, LastUpdatedUtc TIMESTAMP, ADFRunId STRING, LoadDate TIMESTAMP"
        expected_df = self.spark.createDataFrame(
            expected_data, schema=expected_data_schema
        )

        self.assertEqual(
            sorted(expected_df.collect()),
            sorted(existing_df.collect()),
            msg="Records mismatch after load of Rosters UpdateTimestamp table.",
        )

    @classmethod
    def tearDownClass(cls):
        print("Testing TestLoadBronzeTable - tearDownClass...")

        cls.spark.sql(f"DROP DATABASE {cls.tenant} CASCADE")
        cls.dbutils.fs.rm(cls.config_mount_location, True)

        cls.dbutils.fs.refreshMounts()
        all_mounts = cls.dbutils.fs.mounts()
        
        mount_point = f"{cls.config_mount_location}/config"
        if any(mount.mountPoint == mount_point for mount in all_mounts):
            Authorisation.unmount(mount_point)
        mount_point = f"{cls.config_mount_location}/{cls.tenant}"
        if any(mount.mountPoint == mount_point for mount in all_mounts):
            Authorisation.unmount(mount_point)

        Authorisation.get_container_client(cls.config_container).delete_container()
        Authorisation.get_container_client(cls.tenant).delete_container()

        print("Testing TestLoadBronzeTable - tearDownClass... DONE")
