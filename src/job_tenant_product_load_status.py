# Databricks notebook source
# MAGIC %md
# MAGIC This notebook is used to verify the load status by checking, if all Parquet files produced by Azure Data Factory and stored in RAW folder in DataLake container was processed by Databricks jobs. <br/>
# MAGIC <br/>
# MAGIC To check load status for particular <b>Tenant</b> and <b>Product</b>, please: <br/>
# MAGIC set values to variables TENANT_NAME and PRODUCT_NAME below <br/>
# MAGIC and run this notebook. <br/>
# MAGIC <br/>
# MAGIC Resultset of last command will genereta the table with the following columns: <br/>
# MAGIC <br/>
# MAGIC Schema_Name,Table_Name,Table_ADFRunId,Raw_ADFRunId,Table_Checkpoint,Raw_Checkpoint,LatestData,TimeDifferenceMinutes <br/>
# MAGIC audit,addresshistory,20240206123001699,20240206123001699,2024-02-06T12:14:16.351+0000,2024-02-06T12:14:16.351+0000,true,null <br/>
# MAGIC dbo,address,20240206123001699,20240206123001699,2024-02-06T12:20:09.957+0000,2024-02-06T12:20:09.957+0000,true,null <br/>
# MAGIC <br/>
# MAGIC The expected result is that all rows has <b>true</b> value in <b>LatestData</b> column. <br/>
# MAGIC <br/>
# MAGIC <b>Columns description:</b> <br/>
# MAGIC <b>Schema_Name</b> - name of schema in Databricks table <br/>
# MAGIC <b>Table_Name</b> - name of table in Databricks table <br/>
# MAGIC <b>Table_ADFRunId</b> - latest (max) value of ADFRunId in Databricks table <br/>
# MAGIC <b>Raw_ADFRunId</b> - latest (max) value of ADFRunId in checkpoint in RAW folder in Data Lake container <br/>
# MAGIC <b>Table_Checkpoint</b> - latest (max) value of ValidFrom in Databricks table <br/>
# MAGIC <b>Raw_Checkpoint</b> - latesr (max) value of ValidFrom in checkpoint in RAW folder in Data Lake container <br/>
# MAGIC <b>LatestData</b> - that column has two values:  <br/>
# MAGIC         true, when Raw_ADFRunId = Table_ADFRunId,  <br/>
# MAGIC         false when Raw_ADFRunId > Table_ADFRunId <br/>
# MAGIC <b>TimeDifferenceMinutes</b> - this column has values: <br/>
# MAGIC         null, if LatestData = true (all files from RAW folder are loaded into Databricks tables) <br/>
# MAGIC         integer value, which is the time difference between Raw_Checkpoint and Raw_Checkpoint, if LatestData = false (it means not all files are loaded into Databricks table)

# COMMAND ----------

TENANT_NAME = 'sqa1'
PRODUCT_NAME = 'carelink'

# COMMAND ----------

import os
from modules.configuration_utils import ConfigurationUtils

LAYER_NAME =  'bronze'
MOUNT_PREFIX = "/mnt"
CONFIG_PATH = f"{MOUNT_PREFIX}/config"
PRODUCT_CONFIG_FOLDER_PATH = f"{CONFIG_PATH}/product_config"

def generate_sql_product_table_load_status(tenant_name, layer_name, product_name):
    print("Generate Load Status View for tenant: " + tenant_name + ", layer: " + layer_name + " and product: " + product_name)
    
    SQL_STMT = f""

    print("Checking if product file exists in config container...")
    product_config_file = f"/dbfs{PRODUCT_CONFIG_FOLDER_PATH}/{product_name}.json"

    if os.path.isfile(product_config_file):
        print("Reading file: " + product_config_file)

        product_config = ConfigurationUtils.read_config(product_config_file)
        table_number = 0;

        for table in product_config:
            table_number = table_number + 1

            table_type = table["TableType"]
            schema_name = table["SchemaName"].lower()
            table_name = table["TableName"].lower()

            if table_number > 1:
                 SQL_STMT = SQL_STMT + f"UNION ALL \n\n"

            if table["TableType"] == "SystemVersioned":
                history_table_schema_name = table["HistoryTableSchemaName"].lower()
                history_table_name = table["HistoryTableName"].lower()

                # print(f"Generate SQL statement for {table_type} table : {history_table_schema_name}.{history_table_name}")

                SQL_STMT = SQL_STMT + f"""--SYSTEM VERSIONED HISTORY TABLE
                SELECT
                    from_tbl.Schema_Name AS Schema_Name,
                    from_tbl.Table_Name AS Table_Name,
                    ADFRunId_value_tbl AS Table_ADFRunId,
                    ADFRunId_value_ckp AS Raw_ADFRunId,
                    CAST(Checkpoint_tbl AS TIMESTAMP) AS Table_Checkpoint,
                    CAST(Checkpoint_ckp AS TIMESTAMP) AS Raw_Checkpoint,
                    CASE
                        WHEN ADFRunId_value_tbl < ADFRunId_value_ckp THEN FALSE
                        ELSE TRUE
                    END AS LatestData,
                    CASE
                        WHEN ADFRunId_value_tbl < ADFRunId_value_ckp THEN timestampdiff(MINUTE, Checkpoint_tbl, Checkpoint_ckp)
                        ELSE NULL
                    END AS TimeDifferenceMinutes
                FROM
                (
                    SELECT
                        '{history_table_schema_name}' AS Schema_Name,
                        '{history_table_name}' AS Table_Name,
                        '{tenant_name}.{layer_name}_{product_name}_{history_table_schema_name}_{history_table_name}' AS Full_Table_Name_tbl, 
                        max(ADFRunId) AS ADFRunId_value_tbl,
                        max(ValidFrom) AS Checkpoint_tbl
                    FROM `hive_metastore`.`{tenant_name}`.`{layer_name}_{product_name}_{history_table_schema_name}_{history_table_name}`
                ) AS from_tbl
                INNER JOIN
                (
                    SELECT
                        '{history_table_schema_name}' AS Schema_Name,
                        '{history_table_name}' AS Table_Name,
                        '{tenant_name}.{layer_name}_{product_name}_{history_table_schema_name}_{history_table_name}' AS Full_Table_Name_ckp, 
                        ADFRunId AS ADFRunId_value_ckp,
                        RawCheckpoint AS Checkpoint_ckp
                    FROM PARQUET.`dbfs:/mnt/{tenant_name}/raw/{product_name}/{history_table_schema_name}/{history_table_name}/_checkpoint/checkpoint_{tenant_name}_{product_name}_{history_table_schema_name}_{history_table_name}.parquet`
                ) AS from_ckp
                ON from_tbl.Full_Table_Name_tbl = from_ckp.Full_Table_Name_ckp \n\n"""

                # print(f"Generate SQL statement for {table_type} table : {schema_name}.{table_name}")

                SQL_STMT = SQL_STMT + f"UNION ALL \n\n"

                SQL_STMT = SQL_STMT + f"""--SYSTEM VERSIONED LIVE TABLE
                SELECT
                    from_tbl.Schema_Name AS Schema_Name,
                    from_tbl.Table_Name AS Table_Name,
                    ADFRunId_value_tbl AS Table_ADFRunId,
                    ADFRunId_value_ckp AS Raw_ADFRunId,
                    CAST(Checkpoint_tbl AS TIMESTAMP) AS Table_Checkpoint,
                    CAST(Checkpoint_ckp AS TIMESTAMP) AS Raw_Checkpoint,
                    CASE
                        WHEN ADFRunId_value_tbl < ADFRunId_value_ckp THEN FALSE
                        ELSE TRUE
                    END AS LatestData,
                    CASE
                        WHEN ADFRunId_value_tbl < ADFRunId_value_ckp THEN timestampdiff(MINUTE, Checkpoint_tbl, Checkpoint_ckp)
                        ELSE NULL
                    END AS TimeDifferenceMinutes
                FROM
                (
                    SELECT
                        '{schema_name}' AS Schema_Name,
                        '{table_name}' AS Table_Name,
                        '{tenant_name}.{layer_name}_{product_name}_{schema_name}_{table_name}' AS Full_Table_Name_tbl, 
                        max(ADFRunId) AS ADFRunId_value_tbl,
                        max(ValidFrom) AS Checkpoint_tbl
                    FROM `hive_metastore`.`{tenant_name}`.`{layer_name}_{product_name}_{schema_name}_{table_name}`
                ) AS from_tbl
                INNER JOIN
                (
                    SELECT
                        '{schema_name}' AS Schema_Name,
                        '{table_name}' AS Table_Name,
                        '{tenant_name}.{layer_name}_{product_name}_{schema_name}_{table_name}' AS Full_Table_Name_ckp, 
                        ADFRunId AS ADFRunId_value_ckp,
                        RawCheckpoint AS Checkpoint_ckp
                    FROM PARQUET.`dbfs:/mnt/{tenant_name}/raw/{product_name}/{schema_name}/{table_name}/_checkpoint/checkpoint_{tenant_name}_{product_name}_{schema_name}_{table_name}.parquet`
                ) AS from_ckp
                ON from_tbl.Full_Table_Name_tbl = from_ckp.Full_Table_Name_ckp \n\n"""

            elif table["TableType"] == "Reference":
                # print(f"Generate SQL statement for {table_type} table : {schema_name}.{table_name}")

                SQL_STMT = SQL_STMT + f"""--REFERENCE TABLE
                SELECT
                    from_tbl.Schema_Name AS Schema_Name,
                    from_tbl.Table_Name AS Table_Name,
                    ADFRunId_value_tbl AS Table_ADFRunId,
                    ADFRunId_value_ckp AS Raw_ADFRunId,
                    Checkpoint_tbl AS Table_Checkpoint,
                    Checkpoint_ckp AS Raw_Checkpoint,
                    CASE
                        WHEN ADFRunId_value_tbl < ADFRunId_value_ckp THEN FALSE
                        ELSE TRUE
                    END AS LatestData,
                    NULL AS TimeDifference
                FROM
                (
                    SELECT
                        '{schema_name}' AS Schema_Name,
                        '{table_name}' AS Table_Name,
                        '{tenant_name}.{layer_name}_{product_name}_{schema_name}_{table_name}' AS Full_Table_Name_tbl, 
                        max(ADFRunId) AS ADFRunId_value_tbl,
                        NULL AS Checkpoint_tbl
                    FROM `hive_metastore`.`{tenant_name}`.`{layer_name}_{product_name}_{schema_name}_{table_name}`
                ) AS from_tbl
                INNER JOIN
                (
                    SELECT
                        '{schema_name}' AS Schema_Name,
                        '{table_name}' AS Table_Name,
                        '{tenant_name}.{layer_name}_{product_name}_{schema_name}_{table_name}' AS Full_Table_Name_ckp, 
                        max(ADFRunId) AS ADFRunId_value_ckp,
                        NULL AS Checkpoint_ckp
                    FROM PARQUET.`dbfs:/mnt/{tenant_name}/raw/{product_name}/{schema_name}/{table_name}/{tenant_name}_{product_name}_{schema_name}_{table_name}_*.parquet`
                ) AS from_ckp
                ON from_tbl.Full_Table_Name_tbl = from_ckp.Full_Table_Name_ckp \n\n"""
        
        print("Return SELECT statement for: " + tenant_name + ", layer: " + layer_name + " and product: " + product_name + "...")
        return SQL_STMT

    else:
        print("File not found: " + product_config_file)


# COMMAND ----------

SQL_SELECT_STMT = generate_sql_product_table_load_status(TENANT_NAME, LAYER_NAME, PRODUCT_NAME)

display(spark.sql(SQL_SELECT_STMT))
