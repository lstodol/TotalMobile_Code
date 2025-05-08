import logging
import os
from datetime import timedelta
from modules.exceptions import ExceptionCatcher, ProductConfigurationNotFoundException
from pyspark.sql import SparkSession
from modules.authorisation import Authorisation
from modules.configuration_utils import ConfigurationUtils

logger = logging.getLogger(__name__)


class GlobalConfiguration:
    MOUNT_PREFIX = "/mnt"
    CONFIG_CONTAINER = "config"
    LOGS_CONTAINER = "logs"
    DATABASE_FOLDER = "lakehouse"
    CONFIG_PATH = f"{MOUNT_PREFIX}/config"
    LOGS_PATH = f"{MOUNT_PREFIX}/logs"
    ENV_CONFIG_FOLDER_PATH = f"{CONFIG_PATH}/env_config"
    ENV_CONFIG_FOLDER_PATH_HOT = f"{CONFIG_PATH}/env_config/hot"
    PRODUCT_CONFIG_FOLDER_PATH = f"{CONFIG_PATH}/product_config"
    GLOBAL_CONFIG_FOLDER_PATH = f"{CONFIG_PATH}/global_config"
    TENANT_CONFIG_PATH = f"{CONFIG_PATH}/tenant_config/tenant_product.json"
    GLOBAL_JOBS_CONFIG_PATH = f"{GLOBAL_CONFIG_FOLDER_PATH}/jobs.json"
    ENV_TENANT_CONFIG_PATH = f"{ENV_CONFIG_FOLDER_PATH}/tenant_config.json"
    ENV_PRODUCT_CONFIG_PATH = f"{ENV_CONFIG_FOLDER_PATH}/product_config.json"

    def __init__(self, reconfigure: bool = False):
        self.spark = SparkSession.getActiveSession()
        self.mount_container(self.CONFIG_CONTAINER, self.CONFIG_PATH, reconfigure)
        self.mount_container(self.LOGS_CONTAINER, self.LOGS_PATH, reconfigure)

        self.tenant_product_df = (
            self.spark.read.option("multiline", "true")
            .json(self.TENANT_CONFIG_PATH)
            .filter("IsActive")
        )
        # create configuration for runtime
        self.create_tenant_config()
        self.product_config()

        if reconfigure:
            Authorisation.refresh_mounts()

    @classmethod
    def _get_product_configuration(cls, tenant_name, product, db_folder):
        product_configuration = None

        if product == "carelink":
            product_configuration = CarelinkConfiguration(tenant_name, db_folder)
        elif product == "totalmobile":
            product_configuration = TotalMobileConfiguration(tenant_name, db_folder)
        elif product == "scheduler":
            product_configuration = LiveSchedulerConfiguration(tenant_name, db_folder)
        elif product == "datamart":
            product_configuration = DatamartConfiguration(tenant_name, db_folder)
        else:
            raise ProductConfigurationNotFoundException(
                f"Unknown Product found: {product}!"
            )
        return product_configuration

    @classmethod
    def get_tenant_mount_point(cls, tenant_name):
        return f"{cls.MOUNT_PREFIX}/{tenant_name}"

    @classmethod
    def create_tenant_config(cls):
        tenant_dict = {}

        # calling read_config from ConfigClass to read tenant config
        tenant_conf = ConfigurationUtils.read_config(f"{cls.TENANT_CONFIG_PATH}")

         
        # create dict of tenant and product mapping
        logger.info("Creating dictionary from tenant config file...")
        for tenant in tenant_conf:
            if tenant["IsActive"] == True:
                if tenant["TenantName"] in tenant_dict.keys():
                    tenant_dict[tenant["TenantName"]].append(tenant["ProductName"])
                else:
                    tenant_dict[tenant["TenantName"]] = [tenant["ProductName"]]

        # writing config file under env config folder for tenant config
        logger.info("Creating env tenant config file using the dictionary...")
        ConfigurationUtils.write_config(cls.ENV_TENANT_CONFIG_PATH, tenant_dict)

    @classmethod
    def get_job_config(cls, tenant_name: str = None, product_name: str = None) -> dict:
        """Get the configuration for jobs like:
            "NotificationDestinations": [],
            "NotificationRecipients": [
                    "user@email.co.uk"
            ],
            "JobDurationThreshold": 1200,
            "JobTimeoutThreshold": 600,
            "JobRetentionPeriodInDays": 3, ### in days
            "ClusterName": "main_runner"

        Args:
            tenant_name (str, optional): if provided tenant specific configuration is taken. The last one if mulitply products.
            product_name (str, optional): if provided tenant and product specific configuration is taken.

        Returns:
            dict: jobs configuration
        """
        global_jobs_config = ConfigurationUtils.read_config(cls.GLOBAL_JOBS_CONFIG_PATH)
        tenant_config = ConfigurationUtils.read_config(f"{cls.TENANT_CONFIG_PATH}")
        tenant_job_config = {}

        for tenant in tenant_config:
            if tenant["TenantName"] == tenant_name:
                if product_name == None:
                    if "Jobs" in tenant:
                        tenant_job_config = tenant["Jobs"]
                elif tenant["ProductName"] == product_name:
                    if "Jobs" in tenant:
                        tenant_job_config = tenant["Jobs"]

        global_jobs_config.update(tenant_job_config)
        return global_jobs_config

    @classmethod
    def get_tenant_job_retention_period(cls) -> dict:
        job_retention = {}

        # calling read_config from ConfigClass to read tenant config
        tenant_config = ConfigurationUtils.read_config(f"{cls.TENANT_CONFIG_PATH}")

        # create dict of tenant and job retention period in deltatime format
        logger.info(f"Creating dictionary from tenant config: {tenant_config}.")
        for tenant in tenant_config:
            jobs_config = cls.get_job_config(tenant_name=tenant["TenantName"])
            logger.info(
                f"for: {tenant['TenantName']} JobRetentionPeriodInDays is {jobs_config['JobRetentionPeriodInDays']}"
            )
            job_retention[tenant["TenantName"]] = jobs_config[
                "JobRetentionPeriodInDays"
            ]

        return job_retention

    @classmethod
    def product_config(cls):
        product_dict = {}

        logger.info(
            "Checking if product folder exists in config container for product and table mapping..."
        )
        if os.path.isdir(f"/dbfs{cls.PRODUCT_CONFIG_FOLDER_PATH}"):
            logger.info("Found the product config folder...")
            for file in os.listdir(f"/dbfs{cls.PRODUCT_CONFIG_FOLDER_PATH}"):
                if file.endswith(".json"):
                    logger.info(f"Reading product config from {file}...")
                    productConf = ConfigurationUtils.read_config(
                        f"{cls.PRODUCT_CONFIG_FOLDER_PATH}/{file}"
                    )

                    # create dict of product config
                    table_list = []
                    logger.info(f"Creating dictionary of product config for {file}...")
                    for table in productConf:
                        # schema name | table name | table type | primary key | Incremental Column
                        if table["TableType"] == "SystemVersioned":
                            table_list.append(
                                table["SchemaName"]
                                + "|"
                                + table["TableName"]
                                + "|LIVE|"
                                + table["PrimaryKeyColumn"]
                                + "|"
                                + table["IncrementalColumn"]
                            )
                            table_list.append(
                                table["HistoryTableSchemaName"]
                                + "|"
                                + table["HistoryTableName"]
                                + "|HISTORY|"
                                + table["PrimaryKeyColumn"]
                                + "|"
                                + table["IncrementalColumn"]
                            )
                        elif table["TableType"] == "Reference" or table["TableType"] == "HotReference":
                            table_list.append(
                                table["SchemaName"]
                                + "|"
                                + table["TableName"]
                                + "|REFERENCE||"
                            )
                        elif table["TableType"] == "UpdateTimestamp":
                            table_list.append(
                                table["SchemaName"]
                                + "|"
                                + table["TableName"]
                                + "|UPDATETIMESTAMP|"
                                + table["PrimaryKeyColumn"]
                                + "|"
                                + table["IncrementalColumn"]
                            )
                        elif table["TableType"] == "ChangeTrackingVersion":
                            table_list.append(
                                table["SchemaName"]
                                + "|"
                                + table["TableName"]
                                + "|CHANGETRACKINGVERSION|"
                                + table["PrimaryKeyColumn"]
                                + "|"
                            )
                        else:
                            table_list.append(
                                table["SchemaName"] + "|" + table["TableName"] + "|||"
                            )

                    product_dict[file.split(".")[0]] = table_list

        else:
            logger.error("Product folder doesn't exist in config container...")

        # writing config file under env config folder for tenant and product mapping
        logger.info("Creating env product config file using the dictionary...")
        ConfigurationUtils.write_config(f"{cls.ENV_PRODUCT_CONFIG_PATH}", product_dict)

    def init_tenants(self, tenant: str = None):
        """Initialise all tenants in case no specific tenant was provided"""

        tenants_products = self.tenant_product_df
        exception_catcher = ExceptionCatcher(
            context={"activity": "tenant initialisation"}
        )

        if tenant:
            tenants_products = tenants_products.filter(
                tenants_products.TenantName == tenant
            )

        for tenant_row in tenants_products.select("TenantName").distinct().collect():
            tenant_name = str(tenant_row["TenantName"])
            exception_catcher.set_context(tenant_name=tenant_name)
            try:
                self.configure_tenant(tenant_name)

                for row in tenants_products.filter(
                    tenants_products.TenantName == tenant_name
                ).collect():
                    product = str(row["ProductName"])
                    exception_catcher.set_context(product=product)

                    try:
                        self.configure_product(
                            tenant_name,
                            product,
                        )
                    except Exception as e:
                        exception_catcher.catch(e)
                        continue

            except Exception as e:
                exception_catcher.catch(e)
                continue

        exception_catcher.throw_if_any()

    def configure_tenant(self, tenant_name):
        logger.info(f"Configuring tenant: {tenant_name}.")

        mount_location = self.get_tenant_mount_point(tenant_name)
        db_folder = self.DATABASE_FOLDER
        self.mount_container(tenant_name, mount_location)
        self.init_db(tenant_name, mount_location, db_folder)

        logger.info(f"Tenant: {tenant_name} configured successfully.")

    def init_db(self, tenant_name, mount_location, db_folder):
        logger.info("Create tenant DB if not exists.")
        self.spark.sql(
            f"CREATE DATABASE IF NOT EXISTS {tenant_name} LOCATION '{mount_location}/{db_folder}'"
        )

    def configure_product(self, tenant_name, product):
        logger.info(f"Configuring product: {product} for tenant: {tenant_name}.")
        db_folder = self.DATABASE_FOLDER
        product_configuration = self._get_product_configuration(
            tenant_name, product, db_folder
        )
        product_configuration.init_bronze_db()
        logger.info(
            f"Product: {product} for tenant: {tenant_name} configured successfully."
        )

        # walk around to init audit tables and avoid circular dependecy
        tenant_location = f"{GlobalConfiguration.get_tenant_mount_point(tenant_name)}/{self.DATABASE_FOLDER}"
        from modules.databricks_utils import AuditLogger

        audit_logger = AuditLogger(
            tenant=tenant_name, product=product, layer="bronze", action="init"
        )
        audit_logger.init_db(tenant_location)

    def mount_container(self, container_name, mount_point, reconfigure=False):
        client = Authorisation.get_container_client(container_name)
        if not client.exists():
            client.create_container()

        Authorisation.mount(container_name, mount_point, reconfigure)


class BaseProductConfiguration:

    def __init__(self, tenant_name, db_folder):
        self.db_folder = db_folder
        self.tenant_name = tenant_name
        self.bronze_location = f"{GlobalConfiguration.get_tenant_mount_point(self.tenant_name)}/{self.db_folder}/bronze"
        self.product_name = None
        self.spark = SparkSession.getActiveSession()

    def execute_ddl(
        self, bronze_db_schema, table_prefix, bronze_location, history_table_prefix=None
    ):

        with open(bronze_db_schema) as f:
            data = f.read()

        # schema definition file needs to be written to contain dynamic parameters. Each statement needs to be separate by semicolon ';'
        ddl = data.format(
            table_prefix=table_prefix,
            bronze_location=bronze_location,
            history_table_prefix=history_table_prefix,
        )

        for command in ddl.split(";"):
            if len(command) > 0:
                logger.info(f"Executing ddl...{command}.")
                self.spark.sql(command)
                logger.info(f"Successfully completed.")                
                
class CarelinkConfiguration(BaseProductConfiguration):

    def __init__(self, tenant_name, db_folder):
        super().__init__(tenant_name=tenant_name, db_folder=db_folder)
        self.product_name = "carelink"
        self.bronze_db_schema = f"/dbfs{GlobalConfiguration.CONFIG_PATH}/product_config/{self.product_name}_bronze_schema.txt"

        logger.info(
            f"Configuring {self.product_name} product for tenant: {tenant_name}."
        )

    def init_bronze_db(self):
        app_layer = "bronze"

        table_prefix = (
            f"{self.tenant_name}.{app_layer}_{self.product_name}"
        )
        history_table_prefix = (
            f"{self.tenant_name}.{app_layer}_{self.product_name}"
        )

        self.execute_ddl(
            bronze_db_schema=self.bronze_db_schema,
            table_prefix=table_prefix,
            history_table_prefix=history_table_prefix,
            bronze_location=self.bronze_location,
        )


class TotalMobileConfiguration(BaseProductConfiguration):

    def __init__(self, tenant_name, db_folder):
        super().__init__(tenant_name=tenant_name, db_folder=db_folder)
        self.product_name = "totalmobile"
        self.bronze_db_schema = f"/dbfs{GlobalConfiguration.CONFIG_PATH}/product_config/{self.product_name}_bronze_schema.txt"

        logger.info(
            f"Configuring {self.product_name} product for tenant: {tenant_name}."
        )

    def init_bronze_db(self):
        app_layer = "bronze"

        table_prefix = (
            f"{self.tenant_name}.{app_layer}_{self.product_name}"
        )

        self.execute_ddl(
            bronze_db_schema=self.bronze_db_schema,
            table_prefix=table_prefix,
            bronze_location=self.bronze_location,
        )


class LiveSchedulerConfiguration(BaseProductConfiguration):

    def __init__(self, tenant_name, db_folder):
        super().__init__(tenant_name=tenant_name, db_folder=db_folder)
        self.product_name = "scheduler"
        self.bronze_db_schema = f"/dbfs{GlobalConfiguration.CONFIG_PATH}/product_config/{self.product_name}_bronze_schema.txt"

        logger.info(
            f"Configuring '{self.product_name}' product for tenant: {tenant_name}."
        )

    def init_bronze_db(self):
        app_layer = "bronze"

        table_prefix = (
            f"{self.tenant_name}.{app_layer}_{self.product_name}"
        )

        self.execute_ddl(
            bronze_db_schema=self.bronze_db_schema,
            table_prefix=table_prefix,
            bronze_location=self.bronze_location,
        )

class DatamartConfiguration(BaseProductConfiguration):

    def __init__(self, tenant_name, db_folder):
        super().__init__(tenant_name=tenant_name, db_folder=db_folder)
        self.product_name = "datamart"
        self.bronze_db_schema = f"/dbfs{GlobalConfiguration.CONFIG_PATH}/product_config/{self.product_name}_bronze_schema.txt"

        logger.info(
            f"Configuring {self.product_name} product for tenant: {tenant_name}."
        )

    def init_bronze_db(self):
        app_layer = "bronze"

        table_prefix = (
            f"{self.tenant_name}.{app_layer}_{self.product_name}"
        )

        self.execute_ddl(
            bronze_db_schema=self.bronze_db_schema,
            table_prefix=table_prefix,
            bronze_location=self.bronze_location,
        )
