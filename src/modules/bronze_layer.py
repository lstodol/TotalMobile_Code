import logging
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class IngestBronzeTable:
    def __init__(
        self,
        tenant_name,
        product_name,
        schema_name,
        table_name,
        table_type,
        primary_key,
        incremental_column
    ):
        self.spark = SparkSession.getActiveSession()

        self.tenant_name = tenant_name
        self.product_name = product_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.table_type = table_type
        self.primary_key = primary_key
        self.incremental_column = incremental_column
        self.split_char_for_pk = ","

        self.raw_file_location = f"/mnt/{self.tenant_name}/raw/{self.product_name}/{self.schema_name}/{self.table_name}"
        self.checkpoint_location = f"/mnt/{self.tenant_name}/bronze_checkpoint/{self.product_name}/{self.schema_name}/{self.table_name}"
        self.target_table_name = f"{self.tenant_name}.bronze_{self.product_name}_{self.schema_name}_{self.table_name}"

        logger.debug(f"raw_file_location        : {self.raw_file_location}")
        logger.debug(f"checkpoint_location      : {self.checkpoint_location}")
        logger.debug(f"target_table_name        : {self.target_table_name}")
        logger.debug(f"table_type               : {self.table_type}")
        logger.debug(f"primary_key              : {self.primary_key}")
        logger.debug(f"incremental_column       : {self.incremental_column}")

        self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)

    def get_composite_equal_sql(self, source_table, target_table, composite_key):
        sql = ""

        for column in composite_key.split(self.split_char_for_pk):
            clean_column = column.strip()
            sql += f" AND {source_table}.{clean_column} = {target_table}.{clean_column}"

        return sql[5:]

    def process_table_live(self, target_df, source_df, primary_key, incremental_column):
        logger.debug(f"Processing LIVE table...")

        # Get most recent flag only if several actions for a given PK are listed in temp table (incoming file).
        clean_column_list =  [x.strip() for x in primary_key.split(self.split_char_for_pk)]
        if incremental_column == None or len(incremental_column) == 0:
            window_specification = Window.partitionBy(clean_column_list).orderBy(
            source_df.LoadDate.desc()
            )
        else:
            window_specification = Window.partitionBy(clean_column_list).orderBy(
            source_df.LoadDate.desc(), source_df[incremental_column].desc()
            )
        source_df = source_df.withColumn("rn", row_number().over(window_specification))
        source_df = source_df.filter(source_df.rn == 1)

        updates_df_deletes = source_df.filter(source_df.Flag == "D")
        updates_df_deletes = updates_df_deletes.withColumn(
            "LoadDate", to_timestamp("LoadDate")
        ).drop("Flag", "rn")

        updates_df_inserts = source_df.filter(source_df.Flag == "I")
        updates_df_inserts = updates_df_inserts.withColumn(
            "LoadDate", to_timestamp("LoadDate")
        ).drop("Flag", "rn")

        logger.debug(f"Processing LIVE table... Deleting...")

        target_df.alias("target_table").merge(
            updates_df_deletes.alias("updates"),
            self.get_composite_equal_sql("updates", "target_table", primary_key),
        ).whenMatchedDelete().execute()

        logger.debug(f"Processing LIVE table... Upserting...")

        target_df.alias("target_table").merge(
            updates_df_inserts.alias("updates"),
            self.get_composite_equal_sql("updates", "target_table", primary_key),
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        logger.debug(f"Processing LIVE table... DONE")

    def process_table_history(self, target_df, source_df, primary_key):
        logger.debug(f"Processing HISTORY table...")

        updates_df_all = source_df.withColumn(
            "LoadDate", to_timestamp("LoadDate")
        ).drop("Flag")

        logger.debug(f"Processing HISTORY table... Inserting...")

        target_df.alias("target_table").merge(
            updates_df_all.alias("updates"),
            self.get_composite_equal_sql("updates", "target_table", primary_key)
            + " AND updates.ValidTo = target_table.ValidTo",
        ).whenNotMatchedInsertAll().execute()

        logger.debug(f"Processing HISTORY table... DONE")

    def process_table_reference(self, source_df, target_table_name):
        logger.debug(f"Processing REFERENCE table...")

        max_load_date = source_df.agg(max("LoadDate")).collect()[0][0]
        updates_df_all = source_df.filter(source_df.LoadDate == max_load_date)
        updates_df_all = updates_df_all.drop("Flag")

        logger.debug(f"Processing REFERENCE table... Overwriting...")

        updates_df_all.withColumn("LoadDate", to_timestamp("LoadDate")).write.option(
            "mergeSchema", "true"
        ).mode("overwrite").saveAsTable(target_table_name)

        logger.debug(f"Processing REFERENCE table... DONE")
    
    def process_table_UpdateTimestamp(self, target_df, source_df, primary_key, incremental_column):
        logger.debug(f"Processing UpdateTimestamp table...")

        # Get most recent flag only if several actions for a given PK are listed in temp table (incoming file).
        clean_column_list =  [x.strip() for x in primary_key.split(self.split_char_for_pk)]
        if incremental_column == None or len(incremental_column) == 0:
            window_specification = Window.partitionBy(clean_column_list).orderBy(
            source_df.LoadDate.desc()
            )
        else:
            window_specification = Window.partitionBy(clean_column_list).orderBy(
            source_df.LoadDate.desc(), source_df[incremental_column].desc()
            )
        source_df = source_df.withColumn("rn", row_number().over(window_specification))
        source_df = source_df.filter(source_df.rn == 1)

        updates_df_inserts = source_df.filter(source_df.Flag == "IU")
        updates_df_inserts = updates_df_inserts.withColumn(
            "LoadDate", to_timestamp("LoadDate")
        ).drop("Flag", "rn")

        logger.debug(f"Processing UpdateTimestamp table... Upserting...")

        target_df.alias("target_table").merge(
            updates_df_inserts.alias("updates"),
            self.get_composite_equal_sql("updates", "target_table", primary_key),
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        logger.debug(f"Processing UpdateTimestamp table... DONE")  
        
        
    def process_table_ChangeTrackingVersion(self, target_df, source_df, primary_key, incremental_column):
        logger.debug(f"Processing ChangeTrackingVersion table...")

        # Get most recent flag only if several actions for a given PK are listed in temp table (incoming file).
        clean_column_list =  [x.strip() for x in primary_key.split(self.split_char_for_pk)]
        if incremental_column == None or len(incremental_column) == 0:
            window_specification = Window.partitionBy(clean_column_list).orderBy(
            source_df.LoadDate.desc()
            )
        else:
            window_specification = Window.partitionBy(clean_column_list).orderBy(
            source_df.LoadDate.desc(), source_df[incremental_column].desc()
            )
        source_df = source_df.withColumn("rn", row_number().over(window_specification))
        source_df = source_df.filter(source_df.rn == 1)

        updates_df_deletes = source_df.filter(source_df.Flag == "D")
        updates_df_deletes = updates_df_deletes.withColumn(
            "LoadDate", to_timestamp("LoadDate")
        ).drop("Flag", "rn")

        updates_df_inserts = source_df.filter(source_df.Flag == "IU")
        updates_df_inserts = updates_df_inserts.withColumn(
            "LoadDate", to_timestamp("LoadDate")
        ).drop("Flag", "rn")

        logger.debug(f"Processing ChangeTrackingVersion table... Deleting...")

        target_df.alias("target_table").merge(
            updates_df_deletes.alias("updates"),
            self.get_composite_equal_sql("updates", "target_table", primary_key),
        ).whenMatchedDelete().execute()

        logger.debug(f"Processing ChangeTrackingVersion table... Upserting...")

        target_df.alias("target_table").merge(
            updates_df_inserts.alias("updates"),
            self.get_composite_equal_sql("updates", "target_table", primary_key),
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        logger.debug(f"Processing ChangeTrackingVersion table... DONE")    

    def foreach_batch(self, source_df, batchId):
        logger.debug(f"processing batchId: {batchId}")
        logger.debug(f"processing table: {self.target_table_name} as {self.table_type}")
        target_df = DeltaTable.forName(self.spark, self.target_table_name)

        if self.table_type == "LIVE":
            self.process_table_live(target_df, source_df, self.primary_key, self.incremental_column)

        elif self.table_type == "HISTORY":
            self.process_table_history(target_df, source_df, self.primary_key)

        elif self.table_type == "REFERENCE":
            self.process_table_reference(
                source_df,
                self.target_table_name,
            )
            
        elif self.table_type == "UPDATETIMESTAMP":
            self.process_table_UpdateTimestamp(target_df, source_df, self.primary_key, self.incremental_column)
            
        elif self.table_type == "CHANGETRACKINGVERSION":
            self.process_table_ChangeTrackingVersion(target_df, source_df, self.primary_key, self.incremental_column)

    def load(self):
        logger.debug("Reading RAW folder...")

        source_df = (
            self.spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.schemaLocation", self.raw_file_location)
            .option("mergeSchema", "true")
            .load(self.raw_file_location)
        )

        logger.debug("Checking checkpoint...")

        query = (
            source_df.writeStream.format("delta")
            .foreachBatch(self.foreach_batch)
            .option("checkpointLocation", self.checkpoint_location)
            .option("mergeSchema", "true")
            .outputMode("update")
            .trigger(availableNow=True)
            .start()
        )

        query.awaitTermination()

        logger.debug("Data transfering... DONE")
