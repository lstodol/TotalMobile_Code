import logging
import os
import json
from modules.exceptions import ExceptionCatcher, ProductConfigurationNotFoundException, DomainConfigurationNotFoundException
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.dbutils import DBUtils
from modules.authorisation import Authorisation
from modules.configuration_utils import ConfigurationUtils

logger = logging.getLogger(__name__)

class GlobalConfigurationHot:
    MOUNT_PREFIX = "/mnt"
    CONFIG_CONTAINER = "config"
    LOGS_CONTAINER = "logs"
    DATABASE_FOLDER = "lakehouse"
    CONFIG_PATH = f"{MOUNT_PREFIX}/config"
    LOGS_PATH = f"{MOUNT_PREFIX}/logs"
    ENV_CONFIG_FOLDER_PATH_HOT = f"{CONFIG_PATH}/env_config/hot"
    GLOBAL_CONFIG_FOLDER_PATH_HOT = f"{CONFIG_PATH}/global_config/hot"
    DOMAIN_CONFIG_FOLDER_PATH_HOT = f"{CONFIG_PATH}/domain_config/hot"
    PRODUCT_CONFIG_FOLDER_PATH_HOT = f"{CONFIG_PATH}/product_config/hot"
    TENANT_PRODUCT_CONFIG_PATH_HOT = f"{CONFIG_PATH}/tenant_config/hot/tenant_product.json"
    TENANT_DOMAIN_CONFIG_PATH_HOT = f"{CONFIG_PATH}/tenant_config/hot/tenant_domain.json"
    GLOBAL_JOBS_CONFIG_PATH = f"{GLOBAL_CONFIG_FOLDER_PATH_HOT}/jobs.json"
    ENV_TENANT_PRODUCT_CONFIG_PATH_HOT = f"{ENV_CONFIG_FOLDER_PATH_HOT}/tenant_product_config.json"
    ENV_TENANT_DOMAIN_CONFIG_PATH_HOT = f"{ENV_CONFIG_FOLDER_PATH_HOT}/tenant_domain_config.json"
    ENV_PRODUCT_CONFIG_PATH_HOT = f"{ENV_CONFIG_FOLDER_PATH_HOT}/product_config.json"
    ENV_DOMAIN_CONFIG_PATH_HOT = f"{ENV_CONFIG_FOLDER_PATH_HOT}/domain_config.json"

    @classmethod
    def get_dbutils(cls):
        spark = SparkSession.getActiveSession()
        return DBUtils(spark)

    def __init__(self, reconfigure: bool = False):
        self.spark = SparkSession.getActiveSession()

        self.tenant_product_df = (
            self.spark.read.option("multiline", "true")
            .json(self.TENANT_PRODUCT_CONFIG_PATH_HOT)
            .filter("IsActive")
        )

        self.tenant_domain_df = (
            self.spark.read.option("multiline", "true")
            .json(self.TENANT_DOMAIN_CONFIG_PATH_HOT)
            .filter("IsActive")
        )

        self.create_tenant_product_config()
        self.create_tenant_domain_config()
        self.create_product_config()
        self.create_domain_config()

        if reconfigure:
            Authorisation.refresh_mounts()
    
    @classmethod
    def _get_domain_configuration(cls, tenant_name, domain, db_folder):
        domain_configuration = None

        if domain == "insight":
            domain_configuration = InsightConfiguration(tenant_name, db_folder)
        elif domain == "care":
            domain_configuration = CareConfiguration(tenant_name, db_folder)
        else:
            raise DomainConfigurationNotFoundException(
                f"Unknown Domain found: {domain}!"
            )
        return domain_configuration
    
    @classmethod
    def _get_product_configuration(cls, tenant_name, product, db_folder):
        product_configuration = None

        if product == "insight":
            product_configuration = InsightConfiguration(tenant_name, db_folder)
        elif product == "totalmobile":
            product_configuration = TotalmobileConfiguration(tenant_name, db_folder)
        else:
            raise ProductConfigurationNotFoundException(
                f"Unknown Product found: {product}!"
            )
        return product_configuration

    @classmethod
    def get_tenant_mount_point(cls, tenant_name):
        return f"{cls.MOUNT_PREFIX}/{tenant_name}"

    @classmethod
    def create_tenant_domain_config(cls):
        tenant_domain_dict = {}

        domain_conf = ConfigurationUtils.read_config(f"{cls.TENANT_DOMAIN_CONFIG_PATH_HOT}")
        logger.info("Creating dictionary from tenant domain config file...")

        for tenant in domain_conf:
            if tenant["IsActive"] == True:
                if tenant["TenantName"] in tenant_domain_dict.keys():
                    tenant_domain_dict[tenant["TenantName"]].append(tenant["Domain"])
                else:
                    tenant_domain_dict[tenant["TenantName"]] = [tenant["Domain"]]

        logger.info("Creating env tenant domain config file using the dictionary...")
        ConfigurationUtils.write_config(cls.ENV_TENANT_DOMAIN_CONFIG_PATH_HOT, tenant_domain_dict)

    @classmethod
    def create_tenant_product_config(cls):
        tenant_product_dict = {}

        tenant_conf = ConfigurationUtils.read_config(f"{cls.TENANT_PRODUCT_CONFIG_PATH_HOT}")
        logger.info("Creating dictionary from tenant config file...")

        for tenant in tenant_conf:
            if tenant["IsActive"] == True:
                if tenant["TenantName"] in tenant_product_dict.keys():
                    tenant_product_dict[tenant["TenantName"]].append(tenant["ProductName"])
                else:
                    tenant_product_dict[tenant["TenantName"]] = [tenant["ProductName"]]

        logger.info("Creating env tenant product config file using the dictionary...")
        ConfigurationUtils.write_config(cls.ENV_TENANT_PRODUCT_CONFIG_PATH_HOT, tenant_product_dict)

    @classmethod
    def get_job_config(cls, tenant_name: str = None, product_name: str = None, domain_name: str = None) -> dict:
        """Get the configuration for jobs like:
            "NotificationDestinations": [],
            "NotificationRecipients": [
                    "user@email.co.uk"
            ],
            "ClusterName": "main_runner"

        Args:
            tenant_name (str, optional): if provided tenant specific configuration is taken. The last one if mulitply products.
            product_name (str, optional): if provided tenant and product specific configuration is taken.

        Returns:
            dict: jobs configuration
        """
        global_jobs_config = ConfigurationUtils.read_config(cls.GLOBAL_JOBS_CONFIG_PATH)
        tenant_config = ConfigurationUtils.read_config(f"{cls.TENANT_PRODUCT_CONFIG_PATH_HOT}")
        domain_config = ConfigurationUtils.read_config(f"{cls.TENANT_DOMAIN_CONFIG_PATH_HOT}")
        tenant_job_config = {}

        if product_name != "":
            for tenant in tenant_config:
                if tenant["TenantName"] == tenant_name:
                    if product_name == None:
                        if "Jobs" in tenant:
                            tenant_job_config = tenant["Jobs"]
                    elif tenant["ProductName"] == product_name:
                        if "Jobs" in tenant:
                            tenant_job_config = tenant["Jobs"]
        else:
            for tenant in domain_config:
                if tenant["TenantName"] == tenant_name:
                    if domain_name == None:
                        if "Jobs" in tenant:
                            tenant_job_config = tenant["Jobs"]
                    elif tenant["Domain"] == domain_name:
                        if "Jobs" in tenant:
                            tenant_job_config = tenant["Jobs"]

        global_jobs_config.update(tenant_job_config)
        return global_jobs_config

    @classmethod
    def create_product_config(cls):
        product_dict = {}

        logger.info(
            "Checking if product folder exists in config container for product and table mapping..."
        )
        if os.path.isdir(f"/dbfs{cls.PRODUCT_CONFIG_FOLDER_PATH_HOT}"):
            logger.info("Found the product config folder...")
            for file in os.listdir(f"/dbfs{cls.PRODUCT_CONFIG_FOLDER_PATH_HOT}"):
                if file.endswith(".json"):
                    logger.info(f"Reading product config from {file}...")
                    productConf = ConfigurationUtils.read_config(
                        f"{cls.PRODUCT_CONFIG_FOLDER_PATH_HOT}/{file}"
                    )

                    table_list = []
                    logger.info(f"Creating dictionary of product config for {file}...")
                    for table in productConf:

                        table_list.append(
                            f"{table['TableName']}|{table['DataLayer']}|{table['LoadType']}"
                        )
                            
                    product_dict[file.split(".")[0]] = table_list
        else:
            logger.error("Product folder doesn't exist in config container...")

        logger.info("Creating env product config file using the dictionary...")
        ConfigurationUtils.write_config(f"{cls.ENV_PRODUCT_CONFIG_PATH_HOT}", product_dict)
    
    @classmethod
    def create_domain_config(cls):
        domain_dict = {}

        logger.info(
            "Checking if domain folder exists in config container for domain and table mapping..."
        )
        if os.path.isdir(f"/dbfs{cls.DOMAIN_CONFIG_FOLDER_PATH_HOT}"):
            logger.info("Found the domain config folder...")
            for file in os.listdir(f"/dbfs{cls.DOMAIN_CONFIG_FOLDER_PATH_HOT}"):
                if file.endswith(".json"):
                    logger.info(f"Reading domain config from {file}...")
                    domainConf = ConfigurationUtils.read_config(
                        f"{cls.DOMAIN_CONFIG_FOLDER_PATH_HOT}/{file}"
                    )

                    table_list = []
                    logger.info(f"Creating dictionary of domain config for {file}...")
                    for table in domainConf:

                        table_list.append(
                            f"{table['TableName']}|{table['PrimaryKeys']}|{table['DataLayer']}|{table['LoadType']}"
                        )
                            
                    domain_dict[file.split(".")[0]] = table_list
        else:
            logger.error("Domain folder doesn't exist in config container.")

        logger.info("Creating env domain config file using the dictionary...")
        ConfigurationUtils.write_config(f"{cls.ENV_DOMAIN_CONFIG_PATH_HOT}", domain_dict)

    @classmethod  
    def get_event_hub_namespace(cls, tenant:str, product:str):
        tenant_product_df = (
             SparkSession.getActiveSession().read.option("multiline", "true")
                .json(GlobalConfigurationHot.TENANT_PRODUCT_CONFIG_PATH_HOT)
                .filter(col("ProductName") == product)
                .filter(col("TenantName") == tenant)
                .select("EventHubNamespace")
                .limit(1)
        )

        if not tenant_product_df.isEmpty():
            return tenant_product_df.take(1)[0][0]
        
        return None

    def init_tenants(self, tenant: str = None):
        """Initialise all hot tenants in case no specific tenant was provided"""

        tenants_products = self.tenant_product_df
        tenants_domains = self.tenant_domain_df

        exception_catcher = ExceptionCatcher(
            context={"activity": "hot tenant initialisation"}
        )

        if tenant:
            tenants_products = tenants_products.filter(
                tenants_products.TenantName == tenant
            )
            tenants_domains = tenants_domains.filter(
                tenants_domains.TenantName == tenant
            )

        """Intialise product(s) for tenant(s)"""
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

        """Intialise domain(s) for tenant(s)"""
        for tenant_row in tenants_domains.select("TenantName").distinct().collect():
            tenant_name = str(tenant_row["TenantName"])
            exception_catcher.set_context(tenant_name=tenant_name)
            try:
                
                for row in tenants_domains.filter(
                    tenants_domains.TenantName == tenant_name
                ).collect():
                    domain = str(row["Domain"])
                    exception_catcher.set_context(domain=domain)

                    try:
                        self.configure_domain(
                            tenant_name,
                            domain,
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
        db_folder = self.DATABASE_FOLDER
        product_configuration = self._get_product_configuration(tenant_name, product, db_folder)
        product_configuration.init_bronze_layer()

        logger.info(
            f"Product: {product} for tenant: {tenant_name} configured successfully."
        )
        
        #workaround to init audit tables and avoid circular dependecy 
        tenant_location = f"{GlobalConfigurationHot.get_tenant_mount_point(tenant_name)}/{self.DATABASE_FOLDER}"
        from modules.databricks_utils import AuditLogger
        audit_logger = AuditLogger(tenant=tenant_name, product=product, layer="bronze", action="init")
        audit_logger.init_db(tenant_location)

    def configure_domain(self, tenant_name, domain):
        db_folder = self.DATABASE_FOLDER
        domain_configuration = self._get_domain_configuration(tenant_name, domain, db_folder)
        domain_configuration.init_silver_layer()
        domain_configuration.init_gold_layer()

        logger.info(
            f"Domain: {domain} for tenant: {tenant_name} configured successfully."
        )

    def mount_container(self, container_name, mount_point, reconfigure=False):
        client = Authorisation.get_container_client(container_name)
        if not client.exists():
            client.create_container()

        Authorisation.mount(container_name, mount_point, reconfigure)

class BaseConfiguration:

    def __init__(self, tenant_name, db_folder):
        self.db_folder = db_folder
        self.tenant_name = tenant_name
        self.product_name = None

        self.spark = SparkSession.getActiveSession()

    @classmethod
    def get_dbutils(cls):
        spark = SparkSession.getActiveSession()
        return DBUtils(spark)

    def execute_ddl(
        self, db_schema, table_prefix, schema_location
    ):
        
        with open(db_schema) as f:
            data = f.read()

        # schema definition file needs to be written to contain dynamic parameters. Each statement needs to be separate by semicolon ';'
        ddl = data.format(
            table_prefix=table_prefix,
            bronze_location=schema_location,
            silver_location=schema_location,
            gold_location=schema_location
        )

        for command in ddl.split(";"):
            if len(command.strip()) > 0:
                logger.info(f"Executing DDL: {command}.")
                self.spark.sql(command)
                logger.info("Successfully executed DDL.")

class BaseProductConfiguration:

    def __init__(self, tenant_name, db_folder):
        self.db_folder = db_folder
        self.tenant_name = tenant_name
        self.product_name = None

        self.spark = SparkSession.getActiveSession()

    @classmethod
    def get_dbutils(cls):
        spark = SparkSession.getActiveSession()
        return DBUtils(spark)

    def execute_ddl(
        self, db_schema, table_prefix, schema_location
    ):
        
        with open(db_schema) as f:
            data = f.read()

        # schema definition file needs to be written to contain dynamic parameters. Each statement needs to be separate by semicolon ';'
        ddl = data.format(
            table_prefix=table_prefix,
            bronze_location=schema_location,
            silver_location=schema_location,
            gold_location=schema_location
        )

        for command in ddl.split(";"):
            if len(command.strip()) > 0:
                logger.info(f"Executing DDL: {command}.")
                self.spark.sql(command)
                logger.info("Successfully executed DDL.")

class InsightConfiguration(BaseProductConfiguration): 

    def __init__(self, tenant_name, db_folder):
        super().__init__(tenant_name=tenant_name, db_folder=db_folder)
        self.product_name = "insight"
        self.bronze_db_schema = f"/dbfs{GlobalConfigurationHot.PRODUCT_CONFIG_FOLDER_PATH_HOT}/{self.product_name}_bronze_schema.txt"
        self.silver_db_schema = f"/dbfs{GlobalConfigurationHot.PRODUCT_CONFIG_FOLDER_PATH_HOT}/{self.product_name}_silver_schema.txt"
        self.gold_db_schema = f"/dbfs{GlobalConfigurationHot.PRODUCT_CONFIG_FOLDER_PATH_HOT}/{self.product_name}_gold_schema.txt"

        logger.info(
            f"Configuring {self.product_name} product for tenant: {tenant_name}."
        )

    def init_bronze_layer(self):
        app_layer = "bronze"

        table_prefix = (
            f"{self.tenant_name}.{app_layer}_{self.product_name}"
        )
        bronze_location = (
            f"{GlobalConfigurationHot.get_tenant_mount_point(self.tenant_name)}/{self.db_folder}/{app_layer}"
        )

        self.execute_ddl(
            db_schema=self.bronze_db_schema,
            table_prefix=table_prefix,
            schema_location=bronze_location,
        )

    def init_silver_layer(self):
        app_layer = "silver"

        table_prefix = (
            f"{self.tenant_name}.{app_layer}_{self.product_name}"
        )
        silver_location = (
            f"{GlobalConfigurationHot.get_tenant_mount_point(self.tenant_name)}/{self.db_folder}/{app_layer}"
        )

        self.execute_ddl(
            db_schema=self.silver_db_schema,
            table_prefix=table_prefix,
            schema_location=silver_location,
        )

    def init_gold_layer(self):
        app_layer = "gold"

        table_prefix = (
            f"{self.tenant_name}.{app_layer}_{self.product_name}"
        )
        gold_location = (
            f"{GlobalConfigurationHot.get_tenant_mount_point(self.tenant_name)}/{self.db_folder}/{app_layer}"
        )

        self.execute_ddl(
            db_schema=self.gold_db_schema,
            table_prefix=table_prefix,
            schema_location=gold_location,
        )
        
        self.trigger_reference_data_processing()
    
    def trigger_reference_data_processing(self):
        logger.info(f"Triggering notebook for tenant: {self.tenant_name}, product: {self.product_name}")

        dbutils = self.get_dbutils()
        context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
        current_path = context['extraContext']['notebook_path'].rsplit('/', 1)[0]
        reference_data_notebook_path = 'hot/domain/insight/reference_data_main'
        pipeline_script_path = f"{current_path}/{reference_data_notebook_path}"

        try:
            result = dbutils.notebook.run(
                pipeline_script_path,  
                60, 
                {"tenant_name": self.tenant_name, "product_name": self.product_name}
            )
            logger.info(f"Notebook executed for tenant: {self.tenant_name} / product: {self.product_name}. Run result: {result}")
        except Exception as e:
            logger.error(f"Failed to run notebook for tenant: {self.tenant_name} / product: {self.product_name}. Thrown exception: {e}")


class TotalmobileConfiguration(BaseProductConfiguration): 

    def __init__(self, tenant_name, db_folder):
        super().__init__(tenant_name=tenant_name, db_folder=db_folder)
        self.product_name = "totalmobile"
        self.bronze_db_schema = f"/dbfs{GlobalConfigurationHot.PRODUCT_CONFIG_FOLDER_PATH_HOT}/{self.product_name}_bronze_schema.txt"
        self.silver_db_schema = f"/dbfs{GlobalConfigurationHot.PRODUCT_CONFIG_FOLDER_PATH_HOT}/{self.product_name}_silver_schema.txt"
        self.gold_db_schema = f"/dbfs{GlobalConfigurationHot.PRODUCT_CONFIG_FOLDER_PATH_HOT}/{self.product_name}_gold_schema.txt"

        logger.info(
            f"Configuring {self.product_name} product for tenant: {tenant_name}."
        )

    def init_bronze_layer(self):
        app_layer = "bronze"

        table_prefix = (
            f"{self.tenant_name}.{app_layer}_{self.product_name}"
        )
        bronze_location = (
            f"{GlobalConfigurationHot.get_tenant_mount_point(self.tenant_name)}/{self.db_folder}/{app_layer}"
        )

        self.execute_ddl(
            db_schema=self.bronze_db_schema,
            table_prefix=table_prefix,
            schema_location=bronze_location,
        )

    def init_silver_layer(self):
        pass

    def init_gold_layer(self):
        pass


class CareConfiguration(BaseConfiguration): 

    def __init__(self, tenant_name, db_folder):
        super().__init__(tenant_name=tenant_name, db_folder=db_folder)
        self.domain_name = "care"
        self.silver_db_schema = f"/dbfs{GlobalConfigurationHot.DOMAIN_CONFIG_FOLDER_PATH_HOT}/{self.domain_name}_silver_schema.txt"
        self.gold_db_schema = f"/dbfs{GlobalConfigurationHot.DOMAIN_CONFIG_FOLDER_PATH_HOT}/{self.domain_name}_gold_schema.txt"

        logger.info(
            f"Configuring {self.domain_name} domain for tenant: {tenant_name}."
        )

    def init_silver_layer(self):
        app_layer = "silver"

        table_prefix = (
            f"{self.tenant_name}.{app_layer}_{self.domain_name}"
        )
        silver_location = (
            f"{GlobalConfigurationHot.get_tenant_mount_point(self.tenant_name)}/{self.db_folder}/{app_layer}"
        )

        self.execute_ddl(
            db_schema=self.silver_db_schema,
            table_prefix=table_prefix,
            schema_location=silver_location,
        )

    def init_gold_layer(self):
        app_layer = "gold"

        table_prefix = (
            f"{self.tenant_name}.{app_layer}_{self.domain_name}"
        )
        gold_location = (
            f"{GlobalConfigurationHot.get_tenant_mount_point(self.tenant_name)}/{self.db_folder}/{app_layer}"
        )

        self.execute_ddl(
            db_schema=self.gold_db_schema,
            table_prefix=table_prefix,
            schema_location=gold_location,
        )
