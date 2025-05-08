from unittest import TestCase
from unittest.mock import patch
from modules.authorisation import Authorisation
from dbruntime.dbutils import MountInfo


class TestAuthorisation(TestCase):
    @classmethod
    def setUpClass(cls):
        print("Testing authorisation set Up Class.")

    def test_get_dbutils(self):
        dbutils = Authorisation.get_dbutils()
        scopes = [x.name for x in dbutils.secrets.listScopes()]

        self.assertIn("analytics", scopes)

    def test_get_secret(self):
        dbx_principal_id = Authorisation.get_secret("dbx-principal-clientid")

        self.assertTrue(dbx_principal_id, "dbx principal is not present")

    @patch.object(Authorisation, "get_secret", return_value="some_secret")
    @patch.object(Authorisation, "get_dbutils")
    @patch.object(Authorisation, "unmount")
    def test_mount(self, mocked_unmount, mocked_get_dbutils, mocked_get_secret):
        Authorisation.mount("containerName", "mnt/containerName")
        mocked_instance = mocked_get_dbutils.return_value

        mocked_instance.fs.mount.assert_called_once_with(
            source="abfss://containerName@some_secret.dfs.core.windows.net/",
            mount_point="mnt/containerName",
            extra_configs={
                "fs.azure.account.auth.type": "OAuth",
                "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                "fs.azure.account.oauth2.client.id": "some_secret",
                "fs.azure.account.oauth2.client.secret": "some_secret",
                "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/some_secret/oauth2/token",
            },
        )

    @patch.object(Authorisation, "get_secret", return_value="some_secret")
    @patch.object(Authorisation, "get_dbutils")
    @patch.object(Authorisation, "unmount")
    def test_mount_reconfigure(
        self, mocked_unmount, mocked_get_dbutils, mocked_get_secret
    ):
        # arrange
        mocked_instance = mocked_get_dbutils.return_value
        mocked_instance.fs.mounts.return_value = [
            MountInfo(
                mountPoint="mnt/containerName", source="any", encryptionType="any"
            )
        ]

        # act
        Authorisation.mount("containerName", "mnt/containerName", reconfigure=True)

        # assert
        mocked_unmount.assert_called_once_with("mnt/containerName")

    @patch.object(Authorisation, "get_secret", return_value="some_secret")
    @patch.object(Authorisation, "get_dbutils")
    def test_unmount(self, mocked_get_dbutils, mocked_get_secret):
        # act
        Authorisation.unmount("mnt/containerName")

        # assert
        mocked_instance = mocked_get_dbutils.return_value
        mocked_instance.fs.unmount.assert_called_once_with("mnt/containerName")

    @patch.object(Authorisation, "get_blob_client")
    def test_get_container_client(self, mocked_get_blob_client):
        # act
        Authorisation.get_container_client("someContainer")

        # assert
        instance = mocked_get_blob_client.return_value
        instance.get_container_client.assert_called_once_with("someContainer")

    @classmethod
    def tearDownClass(cls):
        print("Cleaning up after authorisation testing...")
