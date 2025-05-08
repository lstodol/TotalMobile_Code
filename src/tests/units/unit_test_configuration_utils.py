import json
import unittest

from modules.configuration_utils import ConfigurationUtils
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

TEST_FOLDER_PATH = "/TestConfigurationUtils"
TEST_FILE_NAME = "TestFile.json"
TEST_FILE_PATH = "/dbfs" + TEST_FOLDER_PATH + "/" + TEST_FILE_NAME
TEST_FILE_BODY = {'key1': ['value1'], 'key2': ['value2']}

class TestConfigurationUtils(unittest.TestCase):
   
    @classmethod
    def setUpClass(cls):
        print("Testing TestConfigurationUtils - setUpClass...")
        
        cls.spark = SparkSession.builder.getOrCreate() 
        cls.dbutils = DBUtils(cls.spark)
        cls.dbutils.fs.mkdirs(TEST_FOLDER_PATH)

        print("Testing TestConfigurationUtils - setUpClass... DONE")

    def test_fix_location_without_dbfs(self):
        file_path = "/test/filepath.txt"
        result = ConfigurationUtils._fix_location(file_path)

        self.assertEqual(f"/dbfs{file_path}", result, msg="filepath does not have valid dbfs prefix.")

    def test_fix_location_with_dbfs(self):
        file_path = "/dbfs/test/filepath.txt"
        result = ConfigurationUtils._fix_location(file_path)

        self.assertEqual(file_path, result, msg="filepath with prefix /dbfs was modified, but it should not.")

    def test_read_config_file_json_incorrect_format(self):        
        print("Test name: test_read_file_json_incorrect_format")

        # write file with incorrect JSON format
        with open(TEST_FILE_PATH, "w") as f:
            f.write(' test: \"number\"')

        with self.assertRaises(json.decoder.JSONDecodeError, msg="Different exception than json.decoder.JSONDecodeError is not expected.") as cm:
            json_read = ConfigurationUtils.read_config(TEST_FILE_PATH)

    def test_read_config_file_json_not_empty(self):
        print("Test name: test_read_file_json_not_empty")

        # write test json file
        json_expected = json.dumps(TEST_FILE_BODY)
        with open(TEST_FILE_PATH, "w") as f:
            f.write(json_expected)

        # read test json file
        json_read = ConfigurationUtils.read_config(TEST_FILE_PATH)        

        self.assertNotEqual(0, len(str(json_read)), "The content of the file is empty.")

    def test_read_config_file_json_not_exist(self):        
        print("Test name: test_read_file_json_not_exist")

        with self.assertRaises(FileNotFoundError, msg="Different exception than FileNotFoundError is not expected.") as cm:
            json_read = ConfigurationUtils.read_config(TEST_FILE_PATH + "_not_exist")
    
    def test_write_config_file_json_check_content(self):
        print("Test name: test_write_file_json_check_content")

        # write test json file
        json_expected = json.dumps(TEST_FILE_BODY)
        ConfigurationUtils.write_config(TEST_FILE_PATH, TEST_FILE_BODY)

        # read test json file
        json_read = ConfigurationUtils.read_config(TEST_FILE_PATH)

        self.assertEqual(TEST_FILE_BODY, json_read, "The read content of the file does not match the written content.")

    def test_write_config_file_json_check_path(self):        
        print("Test name: test_write_file_json_check_path")

        # write test json file
        ConfigurationUtils.write_config(TEST_FILE_PATH, TEST_FILE_BODY)

        # read first char from file, if success --> file exists
        self.assertTrue(len(self.dbutils.fs.head(TEST_FOLDER_PATH + "/" + TEST_FILE_NAME, 1)) == 1, msg="File not found. Expected that file exists.")


    def test_write_config_file_json_path_not_exist_but_it_will_create_it(self):        
        #arrange
        print("Test name: test_write_file_json_path_not_exist_but_it_will_create_it")
        filepath = f"{TEST_FOLDER_PATH}/not_existing_folder" + TEST_FILE_PATH + "_not_exist"
        #act
        ConfigurationUtils.write_config(f"{TEST_FOLDER_PATH}/not_existing_folder" + TEST_FILE_PATH + "_not_exist", TEST_FILE_BODY)
        #assert
        self.assertTrue(len(self.dbutils.fs.head(filepath, 1)) == 1, msg="File not found. Expected that file exists.")

    @classmethod
    def tearDownClass(cls):
        print("Testing TestConfigurationUtils - tearDownClass...")

        cls.dbutils.fs.rm(TEST_FOLDER_PATH, True)

        print("Testing TestConfigurationUtils - tearDownClass... DONE")