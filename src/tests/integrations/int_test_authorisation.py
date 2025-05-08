from unittest import TestCase
from pyspark.sql import SparkSession
from modules.authorisation import Authorisation
from pyspark.dbutils import DBUtils
import os


class TestAuthorisation(TestCase):
    container_name = "intestauthorisation"
    mount_location = f"/mnt/{container_name}"

    @classmethod
    def setUpClass(cls):
        print("Testing authorisation set Up Class.")
        cls.dbutils = DBUtils(SparkSession.getActiveSession())

    def test_mount(self):
        # act

        client = Authorisation.get_container_client(self.container_name)
        if not client.exists():
            client.create_container()

        Authorisation.mount(self.container_name, self.mount_location)

        # assert
        self.assertTrue(os.path.exists(f"/dbfs{self.mount_location}"))
        self.assertTrue(
            any(
                mount.mountPoint == self.mount_location
                for mount in self.dbutils.fs.mounts()
            )
        )

    @classmethod
    def tearDownClass(cls):
        print("Cleaning up after authorisation testing...")

        print("Unmounting all tenant initialisation points")
        cls.dbutils.fs.refreshMounts()
        all_mounts = cls.dbutils.fs.mounts()
        mount_point = f"{cls.mount_location}"
        if any(mount.mountPoint == mount_point for mount in all_mounts):
            Authorisation.unmount(mount_point)
            
        Authorisation.get_container_client(cls.container_name).delete_container()
