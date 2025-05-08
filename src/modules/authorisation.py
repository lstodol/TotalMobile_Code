import logging
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient

logger = logging.getLogger(__name__)

class Authorisation:
    SECRET_SCOPE = "analytics"
    @classmethod
    def get_dbutils(cls):
        spark = SparkSession.getActiveSession()
        return DBUtils(spark)

    @classmethod
    def refresh_mounts(cls):
        dbutils = cls.get_dbutils()
        
        try:
            dbutils.fs.refreshMounts()
            logger.info(f"Mounts refreshed successfully.")
        except Exception as e:
            logger.warning(f"Failed to refresh mounts: {e}")

        

    @classmethod
    def get_secret(cls, key):
        dbutils = cls.get_dbutils()
        return dbutils.secrets.get(scope=cls.SECRET_SCOPE, key=key)

    @classmethod
    def mount(cls, container_name, mount_point, reconfigure=False):
        dbutils = cls.get_dbutils()
        # do not mount if already exists, unless reconfiguration is forced
        if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
            if reconfigure:
                cls.unmount(mount_point)
            else:
                return

        storage_account = cls.get_secret("storage-account-name")
        application_id = cls.get_secret("dbx-principal-clientid")
        tenant_id = cls.get_secret("tenant-id")
        service_credential = cls.get_secret("dbx-principal-secret")

        configs = {
            "fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": f"{application_id}",
            "fs.azure.account.oauth2.client.secret": f"{service_credential}",
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
        }

        source = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/"
        logger.info(f"Mounting location: {source} under: {mount_point}")

        dbutils.fs.mount(source=source, mount_point=mount_point, extra_configs=configs)

        logger.info(f"Location: {source} under: {mount_point} mounted successfully.")

    @classmethod
    def unmount(cls, mount_point):
        dbutils = cls.get_dbutils()
        try:
            dbutils.fs.unmount(mount_point)
        except Exception as e:
            logger.warning(f"Failed to unmount: {mount_point}. Exception details: {e}")

    @classmethod
    def get_blob_client(cls):
        storage_account = cls.get_secret("storage-account-name")
        application_id = cls.get_secret("dbx-principal-clientid")
        tenant_id = cls.get_secret("tenant-id")
        service_credential = cls.get_secret("dbx-principal-secret")

        credentials = ClientSecretCredential(tenant_id=tenant_id, client_id=application_id, client_secret=service_credential)
        return BlobServiceClient(account_url=f"https://{storage_account}.blob.core.windows.net/", credential=credentials)

    @classmethod
    def get_container_client(cls, container_name):
        service = cls.get_blob_client()
        return service.get_container_client(container_name)

