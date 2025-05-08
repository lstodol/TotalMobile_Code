import logging
import pandas as pd
from datetime import datetime, timedelta
from string import Template
from typing import Tuple
from pyspark.sql import SparkSession, Window, DataFrame, functions as F
from pyspark.sql.types import IntegerType, TimestampType, FloatType, DecimalType, LongType
from pyspark.sql.streaming.query import StreamingQuery
from delta.tables import DeltaTable
from storage.repository_interface import RepositoryInterface

logger = logging.getLogger(__name__)


SPARK_SESSION_APP_NAME = "pipeline-hot-path"
SHIFT_KPI_FACT_MAX_HOURS = 24


class Repository(RepositoryInterface):

    DELTA_TABLE_PATH_PREFIX = Template("/mnt/${tenant_name}/lakehouse/${data_layer}")
    DELTA_TABLE_PATH = Template("/${tenant_name}.${data_layer}_${product_name}_${table_name}")
    DELTA_TABLE_CHECKPOINT_PATH = Template("_checkpoint/${tenant_name}.${data_layer}_${product_name}_${table_name}")

    TENANT_CONFIG_TABLE_NAME = Template("${tenant_name}.gold_${product_name}_tenantconfig")
    KPI_TARGET_CONFIG_TABLE_NAME = Template("${tenant_name}.gold_${product_name}_kpitargetconfig")
    KPI_DIMENSION_TABLE_NAME = Template("${tenant_name}.gold_${product_name}_kpidimension")
    SHIFTKPIFACT_TABLE_NAME = Template("${tenant_name}.gold_${product_name}_shiftkpifact")
    SHIFTKPAFACT_TABLE_NAME = Template("${tenant_name}.gold_${product_name}_shiftkpafact")
    SHIFT_DIMENSION_TABLE_NAME = Template("${tenant_name}.gold_${product_name}_shiftdimension")
    SHIFTEXCLUSION_FACT_TABLE_NAME = Template("${tenant_name}.gold_${product_name}_shiftexclusionfact")
    SHIFTEXCLUSIONREASON_DIMENSION_TABLE_NAME = Template("${tenant_name}.gold_${product_name}_shiftexclusionreasondimension")
    SILVER_SHIFT_TABLE_NAME = Template("${tenant_name}.silver_${product_name}_shift")
    SILVER_ACTIVITY_TABLE_NAME = Template("${tenant_name}.silver_${product_name}_activity")
    SILVER_TASK_TABLE_NAME = Template("${tenant_name}.silver_${product_name}_task")
    TASKSTATUS_DIMENSION_TABLE_NAME = Template("${tenant_name}.gold_${product_name}_taskstatusdimension")
    ACTIVITYTYPE_DIMENSION_TABLE_NAME = Template("${tenant_name}.gold_${product_name}_activitytypedimension")
    TIME_DIMENSION_TABLE_NAME = Template("${tenant_name}.gold_${product_name}_timedimension")


    def __init__(self, spark_sess: SparkSession, delta: DeltaTable) -> None:
        super().__init__()

        self._spark: SparkSession = spark_sess
        self._delta: DeltaTable = delta

    
    def _build_table_paths(self, tenant_name: str, product_name: str, data_layer: str, table_name: str) -> Tuple[str, str]:
        delta_table_path_prefix = self.DELTA_TABLE_PATH_PREFIX.substitute(tenant_name=tenant_name, data_layer=data_layer)
        delta_table_path = delta_table_path_prefix + self.DELTA_TABLE_PATH.substitute(data_layer=data_layer, tenant_name=tenant_name, product_name=product_name, table_name=table_name)
        delta_table_checkpoint_path = delta_table_path_prefix + self.DELTA_TABLE_CHECKPOINT_PATH.substitute(data_layer=data_layer, tenant_name=tenant_name, product_name=product_name, table_name=table_name)
        
        return delta_table_path, delta_table_checkpoint_path
    

    def _sync_dimension_table(self, new_dims_df: DataFrame, dimension_table_path: str, source_col: str, target_col: str) -> DataFrame:
        dim_table = DeltaTable.forPath(self._spark, dimension_table_path)

        new_dims_df = new_dims_df.dropna()

        logger.info("NEW DIMS DF")

        dim_table.alias("tgt").merge(
            new_dims_df.alias("src"),
            f"tgt.{target_col} = src.{source_col}"
        ).whenNotMatchedInsert(
            values={
                target_col: F.col(f"src.{source_col}"),
                "load_date": "CURRENT_TIMESTAMP()"
            }
        ).execute()

        return dim_table.toDF()


    def get_latest_tenantconfig(self, tenant_name: str, product_name: str) -> DataFrame:
        tc_tbl_name = self.TENANT_CONFIG_TABLE_NAME.substitute(tenant_name=tenant_name, product_name=product_name)
        tenantconfig_df = self._spark.read.format("delta").table(tc_tbl_name)
        latest_tenantconfig_df = (
            tenantconfig_df
                .join(
                    tenantconfig_df.groupBy(tenantconfig_df.name).agg(F.max(tenantconfig_df.revision_datetime).alias("revision_datetime")), 
                    ["name", "revision_datetime"], 
                    "inner"
                ).select(
                    tenantconfig_df.name,
                    tenantconfig_df.value
                )
        )

        return latest_tenantconfig_df
    

    def get_latest_kpitargetconfig(self, tenant_name: str, product_name: str) -> DataFrame:
        ktc_tbl_name = self.KPI_TARGET_CONFIG_TABLE_NAME.substitute(tenant_name=tenant_name, product_name=product_name)
        kd_tbl_name = self.KPI_DIMENSION_TABLE_NAME.substitute(tenant_name=tenant_name, product_name=product_name)
        kpitarget_df = self._spark.read.format("delta").table(ktc_tbl_name)
        kpidimension_df = self._spark.read.format("delta").table(kd_tbl_name)
        latest_kpitarget_df = (
            kpitarget_df
                .join(
                    kpitarget_df.groupBy(kpitarget_df.kpi_id).agg(F.max(kpitarget_df.revision_datetime).alias("revision_datetime")), 
                    ["kpi_id", "revision_datetime"], 
                    "inner"
                ).join(
                    kpidimension_df, 
                    kpitarget_df.kpi_id == kpidimension_df.id, 
                    "inner"
                ).select(
                    kpitarget_df.kpi_id,
                    kpidimension_df.kpi_code,
                    kpitarget_df.target_value
                )
        )

        return latest_kpitarget_df


    def get_kpidimensions(self, tenant_name: str, product_name: str) -> DataFrame:
        kd_tbl_name = self.KPI_DIMENSION_TABLE_NAME.substitute(tenant_name=tenant_name, product_name=product_name)
        kd_tbl = self._spark.read.format("delta").table(kd_tbl_name)

        return kd_tbl.select(
            F.col("id"), 
            F.col("kpi_code"), 
            F.col("kpa_id")
            )
    
    def get_silvershifttable(self, tenant_name: str, product_name: str) -> DataFrame:
        silvershift_tbl_name = self.SILVER_SHIFT_TABLE_NAME.substitute(tenant_name=tenant_name, product_name=product_name)
        shiftexclusionfact_tbl_name = self.SHIFTEXCLUSION_FACT_TABLE_NAME.substitute(tenant_name=tenant_name, product_name=product_name)

        last_batch_dt = self._spark.sql(f"SELECT COALESCE(MAX(version_start), TIMESTAMP('1900-01-01')) FROM {shiftexclusionfact_tbl_name}").collect()[0][0]

        silvershift_tbl = self._spark.read.format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingTimestamp", last_batch_dt) \
            .table(silvershift_tbl_name) 

        silvershift_tbl = silvershift_tbl.withColumn("last_batch_dt", F.lit(last_batch_dt))

        return silvershift_tbl.select(
            F.col("reference").alias("shift_reference"), 
            F.col("start_datetime_local"), 
            F.col("start_datetime_utc"), 
            F.col("end_datetime_local"), 
            F.col("end_datetime_utc"), 
            F.col("_change_type"), 
            F.col("last_batch_dt"),
            F.col("load_date")
            )

    def get_taskstatusdimension(self, tenant_name: str, product_name: str) -> DataFrame:
        taskstatus_tbl_name = self.TASKSTATUS_DIMENSION_TABLE_NAME.substitute(tenant_name=tenant_name, product_name=product_name)
        taskstatus_tbl = self._spark.read.format("delta") \
            .table(taskstatus_tbl_name)
        
        return taskstatus_tbl.select(
            F.col("id").alias("status_id"), 
            F.col("status"), 
            F.col("detailed_status")
            )
    
    def get_activitytypedimension(self, tenant_name: str, product_name: str) -> DataFrame:
        activitytype_tbl_name = self.ACTIVITYTYPE_DIMENSION_TABLE_NAME.substitute(tenant_name=tenant_name, product_name=product_name)
        activitytype_tbl = self._spark.read.format("delta") \
            .table(activitytype_tbl_name)
        
        return activitytype_tbl.select(
            F.col("id").alias("activity_type_id"), 
            F.col("activity_type"), 
            F.col("productivity_type")
            )
    
    def get_silveractivitytable(self, tenant_name: str, product_name: str) -> DataFrame:
        silveractivity_tbl_name = self.SILVER_ACTIVITY_TABLE_NAME.substitute(tenant_name=tenant_name, product_name=product_name)

        silveractivity_df = self._spark.read.format("delta").table(silveractivity_tbl_name)

        return silveractivity_df.select(
            F.col("reference").alias("activity_reference"),
            F.col("task_reference"),
            F.col("basis"),
            F.col("parent_activity_reference"),
            F.col("start_datetime_local").cast(TimestampType()),
            F.col("start_datetime_utc").cast(TimestampType()),
            F.col("end_datetime_local").cast(TimestampType()),
            F.col("end_datetime_utc").cast(TimestampType()),
            F.col("planned_end_datetime_local").cast(TimestampType()),
            F.col("planned_end_datetime_utc").cast(TimestampType())
            )
    
    def get_silvertasktable(self, tenant_name: str, product_name: str) -> DataFrame:
        silvertask_tbl_name = self.SILVER_TASK_TABLE_NAME.substitute(tenant_name=tenant_name, product_name=product_name)

        silvertask_df = self._spark.read.format("delta").table(silvertask_tbl_name)

        return silvertask_df.select(
            F.col("reference"),
            F.col("outcome_code"),
            F.col("outcome_type"),
            F.col("visit_code"),
            F.col("action_type"),
            F.col("task_type")
        )
    
    def get_shiftexclusionreasondimension(self, tenant_name: str, product_name: str) -> DataFrame:
        s_excl_reason_tbl_name = self.SHIFTEXCLUSIONREASON_DIMENSION_TABLE_NAME.substitute(tenant_name=tenant_name, product_name=product_name)
        s_excl_reason_tbl = self._spark.read.format("delta").table(s_excl_reason_tbl_name)

        return s_excl_reason_tbl.select(
            F.col("id"), 
            F.col("reason_code")
        )
    
    def get_shiftdimensions(self, tenant_name: str, product_name: str) -> DataFrame:
        shift_tbl_name = self.SHIFT_DIMENSION_TABLE_NAME.substitute(tenant_name=tenant_name, product_name=product_name)
        shift_tbl = self._spark.read.format("delta").table(shift_tbl_name)

        return shift_tbl.select(
            F.col("id"), 
            F.col("shift_reference")
        )
    
    def read_stream(self, source_table_name: str, data_layer: str, tenant_name: str, product_name: str) -> DataFrame: 
        table_path, _ = self._build_table_paths(
            tenant_name=tenant_name,
            product_name=product_name,
            data_layer=data_layer,
            table_name=source_table_name
        )

        logger.info(f"Opening stream on table '{table_path}'")
         
        return self._spark.readStream \
            .format("delta") \
            .option("readChangeFeed", "true") \
            .load(table_path)

    
    def write_stream_shift(self, df: DataFrame, tenant_name: str, product_name: str) -> StreamingQuery:
        table_path, checkpoint_path = self._build_table_paths(
            tenant_name=tenant_name,
            product_name=product_name,
            data_layer="silver",
            table_name="shift"
        )


        def process_batch(source_df: DataFrame, batchId):
            logger.info(f"Processing batchId: {batchId}")

            target_df = self._delta.forPath(self._spark, table_path)

            ## TODO: shouldnt use UpdateAll(), this would reset the load_date which we dont want
            target_df.alias("tgt").merge(
                source_df.alias("src"),
                "tgt.reference = src.reference"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
            

        return (
            df.writeStream.format("delta")
            .outputMode("update")
            .queryName(f"{tenant_name}-{product_name}-silver-shift-stream")
            .option("checkpointLocation", checkpoint_path)
            .foreachBatch(process_batch)
            .start(table_path)
        )
    

    def write_stream_activity(self, df: DataFrame, tenant_name: str, product_name: str) -> StreamingQuery:
        table_path, checkpoint_path = self._build_table_paths(
            tenant_name=tenant_name,
            product_name=product_name,
            data_layer="silver",
            table_name="activity"
        )

        def process_batch(batch_df, batch_id):
            logger.info(f"Processing batch {batch_id}")

            activity_df = batch_df \
                .withColumn("start_datetime_local_temp", F.when(batch_df.basis == 1, batch_df.start_datetime_local)) \
                .withColumn("end_datetime_local_temp", F.when(batch_df.basis == 1, batch_df.finish_datetime_local)) \
                .withColumn("planned_start_datetime_local", F.when(batch_df.basis == 0, batch_df.start_datetime_local)) \
                .withColumn("planned_end_datetime_local", F.when(batch_df.basis == 0, batch_df.finish_datetime_local)) \
                .withColumn("start_latitude_temp", F.when(batch_df.basis == 1, batch_df.start_latitude)) \
                .withColumn("start_longitude_temp", F.when(batch_df.basis == 1, batch_df.start_longitude)) \
                .withColumn("start_location_validity_temp", F.when(batch_df.basis == 1, batch_df.start_location_validity)) \
                .withColumn("end_latitude_temp", F.when(batch_df.basis == 1, batch_df.finish_latitude)) \
                .withColumn("end_longitude_temp", F.when(batch_df.basis == 1, batch_df.finish_longitude)) \
                .withColumn("end_location_validity_temp", F.when(batch_df.basis == 1, batch_df.finish_location_validity)) \
                .withColumn("planned_start_latitude", F.when(batch_df.basis == 0, batch_df.start_latitude)) \
                .withColumn("planned_longitude", F.when(batch_df.basis == 0, batch_df.start_longitude)) \
                .withColumn("planned_start_location_validity", F.when(batch_df.basis == 0, batch_df.start_location_validity)) \
                .withColumn("planned_end_latitude", F.when(batch_df.basis == 0, batch_df.finish_latitude)) \
                .withColumn("planned_end_longitude", F.when(batch_df.basis == 0, batch_df.finish_longitude)) \
                .withColumn("planned_end_location_validity", F.when(batch_df.basis == 0, batch_df.finish_location_validity)) 

            activity_df = activity_df \
                .withColumn("start_datetime_utc", F.to_utc_timestamp("start_datetime_local_temp", activity_df.timezone)) \
                .withColumn("end_datetime_utc", F.to_utc_timestamp("end_datetime_local_temp", activity_df.timezone)) \
                .withColumn("planned_start_datetime_utc", F.to_utc_timestamp("planned_start_datetime_local", activity_df.timezone)) \
                .withColumn("planned_end_datetime_utc", F.to_utc_timestamp("planned_end_datetime_local", activity_df.timezone)) 

            # Filter to keep only the latest record per reference in the source df
            window_spec = Window.partitionBy("reference").orderBy(F.desc("version"))

            activity_df = activity_df.withColumn("rank", F.row_number().over(window_spec))
            activity_df = activity_df.filter(F.col("rank") == 1).drop("rank")

            silver_activity_table = self._delta.forPath(self._spark, table_path)

            silver_activity_table.alias("tgt").merge(
                activity_df.alias("src"),
                "src.reference = tgt.reference"
            ).whenMatchedUpdate(
                condition="src.version > tgt.version",
                set={
                    "resource_reference": "src.resource_reference",
                    "parent_activity_reference": "src.parent_activity_reference",
                    "task_reference": "src.task_reference",
                    "start_datetime_local": "src.start_datetime_local_temp",
                    "start_datetime_utc": "src.start_datetime_utc",
                    "end_datetime_local": "src.end_datetime_local_temp",
                    "end_datetime_utc": "src.end_datetime_utc",
                    "start_latitude": "src.start_latitude_temp",
                    "start_longitude": "src.start_longitude_temp",
                    "start_location_validity": "src.start_location_validity_temp",
                    "end_latitude": "src.end_latitude_temp",
                    "end_longitude": "src.end_longitude_temp",
                    "end_location_validity": "src.end_location_validity_temp",
                    "planned_start_datetime_local": "src.planned_start_datetime_local",
                    "planned_start_datetime_utc": "src.planned_start_datetime_utc",
                    "planned_end_datetime_local": "src.planned_end_datetime_local",
                    "planned_end_datetime_utc": "src.planned_end_datetime_utc",
                    "planned_start_latitude": "src.planned_start_latitude",
                    "planned_start_longitude": "src.planned_start_longitude",
                    "planned_start_location_validity": "src.planned_start_location_validity",
                    "planned_end_latitude": "src.planned_end_latitude",
                    "planned_end_longitude": "src.planned_end_longitude",
                    "planned_end_location_validity": "src.planned_end_location_validity",
                    "type": "src.type",
                    "version": "src.version",
                    "load_date": "src.load_date"
                }
            ).whenNotMatchedInsert(
                values={
                    "reference": "src.reference",
                    "resource_reference": "src.resource_reference",
                    "parent_activity_reference": "src.parent_activity_reference",
                    "task_reference": "src.task_reference",
                    "start_datetime_local": "src.start_datetime_local_temp",
                    "start_datetime_utc": "src.start_datetime_utc",
                    "end_datetime_local": "src.end_datetime_local_temp",
                    "end_datetime_utc": "src.end_datetime_utc",
                    "start_latitude": "src.start_latitude_temp",
                    "start_longitude": "src.start_longitude_temp",
                    "start_location_validity": "src.start_location_validity_temp",
                    "end_latitude": "src.end_latitude_temp",
                    "end_longitude": "src.end_longitude_temp",
                    "end_location_validity": "src.end_location_validity_temp",
                    "planned_start_datetime_local": "src.planned_start_datetime_local",
                    "planned_start_datetime_utc": "src.planned_start_datetime_utc",
                    "planned_end_datetime_local": "src.planned_end_datetime_local",
                    "planned_end_datetime_utc": "src.planned_end_datetime_utc",
                    "planned_start_latitude": "src.planned_start_latitude",
                    "planned_start_longitude": "src.planned_start_longitude",
                    "planned_start_location_validity": "src.planned_start_location_validity",
                    "planned_end_latitude": "src.planned_end_latitude",
                    "planned_end_longitude": "src.planned_end_longitude",
                    "planned_end_location_validity": "src.planned_end_location_validity",
                    "type": "src.type",
                    "version": "src.version",
                    "load_date": "src.load_date"
                }
            ).execute()

        return (
            df.writeStream \
                .format("delta") \
                .outputMode("update") \
                .queryName(f"{tenant_name}-{product_name}-silver-activity-stream") \
                .option("checkpointLocation", checkpoint_path) \
                .foreachBatch(process_batch) \
                .trigger(processingTime="10 seconds") \
                .start(table_path)
        )

    def write_stream_task(self, df: DataFrame, tenant_name: str, product_name: str) -> None:
        table_path, checkpoint_path = self._build_table_paths(tenant_name, product_name, "silver", "task")

        def process_batch(batch_df: DataFrame, batch_id):
            task_table = self._delta.forPath(self._spark, table_path)

            # Add a ranking column to determine the latest record per reference
            window_spec = Window.partitionBy("reference").orderBy(F.desc("task_version"))
            batch_df = batch_df.withColumn("ranking", F.row_number().over(window_spec))

            task_table.alias("tgt").merge(
                batch_df.alias("src"),
                "tgt.reference = src.reference"
            ).whenMatchedUpdate(
                condition="COALESCE(src.task_version, -1) > COALESCE(tgt.task_version, -1)",
                set={
                    "created_datetime_local": "src.created_datetime_local",
                    "created_datetime_utc": "src.created_datetime_utc",
                    "visit_code": "src.visit_code",
                    "visit_count": "src.visit_count",
                    "task_type": "src.task_type",
                    "class": "src.class",
                    "duration": "src.duration",
                    "rank": "src.rank",
                    "task_version": "src.task_version",
                }
            ).whenNotMatchedInsert(
                values={
                    "reference": "src.reference",
                    "resource_reference": "src.resource_reference",
                    "allocation_version": "src.allocation_version",
                    "created_datetime_utc": "src.created_datetime_utc",
                    "planned_start_datetime_local": "src.planned_start_datetime_local",
                    "planned_start_datetime_utc": "src.planned_start_datetime_utc",
                    "outcome_code": "src.outcome_code",
                    "outcome_type": "src.outcome_type",
                    "visit_code": "src.visit_code",
                    "visit_count": "src.visit_count",
                    "action_type": "src.action_type",
                    "action_version": "src.action_version",
                    "task_type": "src.task_type",
                    "class": "src.class",
                    "duration": "src.duration",
                    "rank": "src.rank",
                    "task_version": "src.task_version",
                    "load_date": "src.load_date"
                }
            ).execute()

        (
            df.writeStream
                .format("delta")
                .outputMode("update")
                .queryName(f"{tenant_name}-{product_name}-silver-task-stream")
                .option("checkpointLocation", checkpoint_path)
                .foreachBatch(process_batch)
                .start(table_path)
        )


    def write_stream_task_allocation(self, df: DataFrame, tenant_name: str, product_name: str) -> None:
        table_path, checkpoint_path = self._build_table_paths(tenant_name, product_name, "silver", "task")
        checkpoint_path = checkpoint_path + "_allocation" ## TODO: add method of differentiating the checkpoint path in build function

        def process_batch(batch_df: DataFrame, batch_id):
            task_table = self._delta.forPath(self._spark, table_path)

            window_spec = Window.partitionBy("allocation_reference").orderBy(F.desc("allocation_version"))
            batch_df = batch_df.withColumn("ranking", F.row_number().over(window_spec))

            task_table.alias("tgt").merge(
                batch_df.alias("src"),
                "tgt.reference = src.reference"
            ).whenMatchedUpdate(
                condition="COALESCE(src.allocation_version, -1) > COALESCE(tgt.allocation_version, -1)",
                set={
                    "resource_reference": "src.resource_reference",
                    "allocation_version": "src.allocation_version",
                    "planned_start_datetime_local": "src.planned_start_datetime_local",
                    "planned_start_datetime_utc": "src.planned_start_datetime_utc",
                }
            ).whenNotMatchedInsertAll().execute()

        (
            df.writeStream
                .format("delta")
                .outputMode("update")
                .queryName(f"{tenant_name}-{product_name}-silver-task_allocation-stream")
                .option("checkpointLocation", checkpoint_path)
                .foreachBatch(process_batch)
                .start(table_path)
        )


    def write_stream_task_action(self, df: DataFrame, tenant_name: str, product_name: str) -> None:
        table_path, checkpoint_path = self._build_table_paths(tenant_name, product_name, "silver", "task")
        checkpoint_path = checkpoint_path + "_action" ## TODO: add method of differentiating the checkpoint path in build function

        def process_batch(batch_df: DataFrame, batch_id):
            task_table = self._delta.forPath(self._spark, table_path)

            # Determine the latest record per reference
            window_spec = Window.partitionBy("action_reference").orderBy(F.desc("action_version"))
            batch_df = batch_df.withColumn("ranking", F.row_number().over(window_spec))
            batch_df = batch_df.filter(F.col("ranking") == 1).drop("ranking")

            task_table.alias("tgt").merge(
                batch_df.alias("src"),
                "tgt.reference = src.reference"
            ).whenMatchedUpdate(
                condition="COALESCE(src.action_version, -1) > COALESCE(tgt.action_version, -1)",
                set={
                    "outcome_code": "src.outcome_code",
                    "outcome_type": "src.outcome_type",
                    "action_type": "src.action_type",
                    "action_version": "src.action_version",
                }
            ).whenNotMatchedInsertAll().execute()

        (
            df.writeStream
                .format("delta")
                .outputMode("update")
                .queryName(f"{tenant_name}-{product_name}-silver-task_action-stream")
                .option("checkpointLocation", checkpoint_path)
                .foreachBatch(process_batch)
                .start(table_path)
        )


    def write_stream_milestone(self, df: DataFrame, tenant_name: str, product_name: str) -> None:
        table_path, checkpoint_path = self._build_table_paths(tenant_name, product_name, "silver", "milestone")

        def process_batch(batch_df: DataFrame, batch_id):
            miles_table = self._delta.forPath(self._spark, table_path)

            # Determine the latest record per reference
            window_spec = Window.partitionBy("reference").orderBy(F.desc("version"))
            batch_df = batch_df.withColumn("rank", F.row_number().over(window_spec))
            batch_df = batch_df.filter(F.col("rank") == 1).drop("rank")

            miles_table.alias("tgt").merge(
                batch_df.alias("src"), 
                "src.reference = tgt.reference"
            ).whenMatchedUpdateAll(
                condition="src.version > tgt.version"
            ).whenNotMatchedInsertAll().execute()

        return (
            df.writeStream.format("delta")
            .outputMode("update")
            .queryName(f"{tenant_name}-{product_name}-silver-milestone-stream")
            .option("checkpointLocation", checkpoint_path)
            .foreachBatch(process_batch)
            .trigger(processingTime="10 seconds")
            .start(table_path)
        )


    def write_stream_shiftfact(self, df: DataFrame, tenant_name: str, product_name: str) -> StreamingQuery:
        shiftfact_table_path, shiftfact_table_checkpoint_path = self._build_table_paths(tenant_name, product_name, "gold", "shiftfact")

        def process_batch(batch_df: DataFrame, batch_id):
            logger.info(f"Processing batch '{batch_id}' of 'shiftfact'")

            batch_dt = datetime.utcnow()
            # update shift dimension tables
            shift_refs_df = batch_df.select(F.col("shift_reference")).distinct()
            shift_dim_table_path, _ = self._build_table_paths(
                tenant_name=tenant_name,
                product_name=product_name,
                data_layer="gold",
                table_name="shiftdimension"
            )
            shift_dims_df = self._sync_dimension_table(
                new_dims_df=shift_refs_df,
                dimension_table_path=shift_dim_table_path,
                source_col="shift_reference",
                target_col="shift_reference"
            )

            # update worker dimension tables
            worker_refs_df = batch_df.select(F.col("worker_reference")).distinct()
            worker_dim_table_path, _ = self._build_table_paths(
                tenant_name=tenant_name,
                product_name=product_name,
                data_layer="gold",
                table_name="workerdimension"
            )
            worker_dims_df = self._sync_dimension_table(
                new_dims_df=worker_refs_df,
                dimension_table_path=worker_dim_table_path,
                source_col="worker_reference",
                target_col="worker_reference"
            )

            logger.info("WORKER REFS DF")
            
            worker_refs_df.show()

            batch_df = batch_df \
                .withColumn("version_start", F.lit(batch_dt)) \
                .withColumn("version_end", F.lit("9999-12-31 23:59:59").cast(TimestampType()))

            new_shiftfacts_df = batch_df \
                .join(
                    shift_dims_df, 
                    batch_df["shift_reference"] == shift_dims_df["shift_reference"], 
                    "left"
                ) \
                .join(
                    worker_dims_df,
                    batch_df["worker_reference"] == worker_dims_df["worker_reference"],
                    "left"
                ) \
                .select(
                    shift_dims_df["id"].cast(IntegerType()).alias("shift_id"),
                    worker_dims_df["id"].cast(IntegerType()).alias("worker_id"),
                    batch_df["shift_start_datetime"],
                    batch_df["shift_start_datetime_utc"],
                    batch_df["shift_start_date_id"].cast(IntegerType()),
                    batch_df["shift_start_date_utc_id"].cast(IntegerType()),
                    batch_df["shift_end_datetime"],
                    batch_df["shift_end_datetime_utc"],
                    batch_df["shift_end_date_id"].cast(IntegerType()),
                    batch_df["shift_end_date_utc_id"].cast(IntegerType()),
                    batch_df["paid_minutes"],
                    batch_df["version_start"],
                    batch_df["version_end"]
                )
            

            shiftfact_table = self._delta.forPath(self._spark, shiftfact_table_path)
            shiftfact_table.alias("tgt").merge(
                new_shiftfacts_df.alias("src"),
                "tgt.shift_id = src.shift_id AND tgt.version_end = \"9999-12-31 23:59:59\""
            ).whenMatchedUpdate(
                set={
                    "version_end": F.expr("src.version_start - INTERVAL 1 SECOND")
                }
            ).execute()

            # write all new/updated shift facts (all will have version_end of 9999-12-31 23:59:59)
            new_shiftfacts_df.write \
                .format("delta") \
                .mode("append") \
                .save(shiftfact_table_path)
            
        
        return df.writeStream \
            .format("delta") \
            .outputMode("update") \
            .queryName(f"{tenant_name}-{product_name}-gold-shiftfact-stream") \
            .option("checkpointLocation", shiftfact_table_checkpoint_path) \
            .foreachBatch(process_batch) \
            .start(shiftfact_table_path)
    
    def write_stream_activitytimelinefact(self, df: DataFrame, tenant_name: str, product_name: str) -> StreamingQuery:
        table_path, table_checkpoint_path = self._build_table_paths(tenant_name, product_name, "gold", "activitytimelinefact")
        
        def process_batch(batch_df: DataFrame, batch_id):
            logger.info(f"Processing batch '{batch_id}' of 'activitytimelinefact'")

            batch_dt = datetime.utcnow()

            timelinecontexttypedimension_table_path, _ = self._build_table_paths(
                tenant_name=tenant_name,
                product_name=product_name,
                data_layer="gold",
                table_name="timelinecontexttypedimension"
            )

            timelinecontexttypedimension_df = self._spark.read.format("delta").load(timelinecontexttypedimension_table_path)
            
            shiftfact_table_path, _ = self._build_table_paths(
                tenant_name=tenant_name,
                product_name=product_name,
                data_layer="gold",
                table_name="shiftfact"
            )
            
            shiftfact_df = self._spark.read.format("delta").load(shiftfact_table_path) \
                .filter(F.col("version_end") == '9999-12-31 23:59:59') \
                .withColumnRenamed("shift_id", "shiftfact_shift_id") \
                .drop("version_start", "version_end")

            combined_df = batch_df \
                .join(
                    shiftfact_df,
                    batch_df.shift_id == shiftfact_df.shiftfact_shift_id, 
                    "left"
                ) 

            combined_df = combined_df \
                .withColumn(
                    "timeline_context_type",
                    F.when(
                        (combined_df.version_start < combined_df.shift_start_datetime_utc) | 
                        (combined_df.shift_start_datetime_utc.isNull()), 
                        "Planned Work (Static)")
                    .when(
                        (combined_df.version_start > combined_df.shift_start_datetime_utc) &
                        (combined_df.activity_start_datetime_utc.isNull()), 
                        "Planned Work (Dynamic)"
                    )
                    .otherwise("Actual Work")
                )
            
            combined_df = combined_df \
                .withColumn("version_start", F.lit(batch_dt)) \
                .withColumn("version_end", F.lit("9999-12-31 23:59:59").cast(TimestampType())) 

            combined_df = combined_df \
                .join(timelinecontexttypedimension_df, "timeline_context_type") \
                .select(
                    combined_df.activity_id,
                    combined_df.shift_id,
                    timelinecontexttypedimension_df.id.alias("activity_timeline_context_type_id"),
                    combined_df.activity_type_id,
                    F.when(combined_df.timeline_context_type.like("Planned%"), combined_df.planned_activity_start_datetime_utc)
                        .otherwise(combined_df.activity_start_datetime_utc)
                        .alias("activity_start_datetime_utc"),
                    F.when(combined_df.timeline_context_type.like("Planned%"), combined_df.planned_activity_end_datetime_utc)
                        .otherwise(combined_df.activity_end_datetime_utc)
                        .alias("activity_end_datetime_utc"),
                    combined_df.version_start,
                    combined_df.version_end
                    )
            
            target_table = self._delta.forPath(self._spark, table_path)

            target_table.alias("tgt").merge(
                combined_df.alias("src"),
                "src.activity_id = tgt.activity_id AND tgt.activity_timeline_context_type_id = src.activity_timeline_context_type_id AND tgt.version_end = \"9999-12-31 23:59:59\""
            ).whenMatchedUpdate(
                set={
                    "version_end": F.expr("src.version_start - INTERVAL 1 SECOND")
                }
            ).execute()

            combined_df.write \
                .format("delta") \
                .mode("append") \
                .save(table_path)

        return df.writeStream \
            .format("delta") \
            .outputMode("update") \
            .queryName(f"{tenant_name}-{product_name}-gold-activitytimelinefact-table-stream") \
            .option("checkpointLocation", table_checkpoint_path) \
            .foreachBatch(process_batch) \
            .start(table_path)
    

    def write_stream_activityfact(self, df: DataFrame, tenant_name: str, product_name: str) -> StreamingQuery:
        table_path, checkpoint_path = self._build_table_paths(tenant_name, product_name, "gold", "activityfact")

        def process_batch(batch_df: DataFrame, batch_id):
            logger.info(f"Processing batchId: {batch_id}")

            batch_dt = datetime.utcnow()

            activitydim_table_path, _ = self._build_table_paths(tenant_name, product_name, "gold", "activitydimension")
            activity_refs_df = batch_df.select(batch_df.activity_reference).distinct()
            activity_dims_df = self._sync_dimension_table(
                new_dims_df=activity_refs_df,
                dimension_table_path=activitydim_table_path,
                source_col="activity_reference",
                target_col="activity_reference"
            )

            workerdim_table_path, _ = self._build_table_paths(tenant_name, product_name, "gold", "workerdimension")
            worker_refs_df = batch_df.select(batch_df.worker_reference).distinct()
            worker_dims_df = self._sync_dimension_table(
                new_dims_df=worker_refs_df,
                dimension_table_path=workerdim_table_path,
                source_col="worker_reference",
                target_col="worker_reference"
            )

            shiftdim_table_path, _ = self._build_table_paths(tenant_name, product_name, "gold", "shiftdimension")
            shift_refs_df = batch_df.select(batch_df.shift_reference).distinct()
            shift_dims_df = self._sync_dimension_table(
                new_dims_df=shift_refs_df,
                dimension_table_path=shiftdim_table_path,
                source_col="shift_reference",
                target_col="shift_reference"
            )

            taskdim_table_path, _ = self._build_table_paths(tenant_name, product_name, "gold", "taskdimension")
            task_refs_df = batch_df.select(batch_df.task_reference).distinct()
            task_dims_df = self._sync_dimension_table(
                new_dims_df=task_refs_df,
                dimension_table_path=taskdim_table_path,
                source_col="task_reference",
                target_col="task_reference"
            )

            batch_df = batch_df \
                .withColumn("version_start", F.lit(batch_dt)) \
                .withColumn("version_end", F.lit("9999-12-31 23:59:59").cast(TimestampType()))

            new_activityfacts_df = batch_df \
                .join(activity_dims_df, "activity_reference", "left") \
                .join(shift_dims_df, "shift_reference", "left") \
                .join(worker_dims_df, "worker_reference", "left") \
                .join(task_dims_df, "task_reference", "left") \
                .select(
                    activity_dims_df.id.cast(IntegerType()).alias("activity_id"),
                    shift_dims_df.id.cast(IntegerType()).alias("shift_id"),
                    worker_dims_df.id.cast(IntegerType()).alias("worker_id"),
                    task_dims_df.id.cast(IntegerType()).alias("task_id"),
                    batch_df.activity_start_datetime,
                    batch_df.activity_start_datetime_utc,
                    batch_df.activity_start_date_id.cast(IntegerType()),
                    batch_df.activity_start_time_id.cast(IntegerType()),
                    batch_df.activity_start_date_utc_id.cast(IntegerType()),
                    batch_df.activity_end_datetime,
                    batch_df.activity_end_datetime_utc,
                    batch_df.activity_end_date_id.cast(IntegerType()),
                    batch_df.activity_end_time_id.cast(IntegerType()),
                    batch_df.activity_end_date_utc_id.cast(IntegerType()),
                    batch_df.activity_start_latitude,
                    batch_df.activity_start_longitude,
                    batch_df.activity_start_location_validity,
                    batch_df.activity_end_latitude,
                    batch_df.activity_end_longitude,
                    batch_df.activity_end_location_validity,
                    batch_df.planned_activity_start_datetime,
                    batch_df.planned_activity_start_datetime_utc,
                    batch_df.planned_activity_start_date_id.cast(IntegerType()),
                    batch_df.planned_activity_start_date_utc_id.cast(IntegerType()),
                    batch_df.planned_activity_end_datetime,
                    batch_df.planned_activity_end_datetime_utc,
                    batch_df.planned_activity_end_date_id.cast(IntegerType()),
                    batch_df.planned_activity_end_date_utc_id.cast(IntegerType()),
                    batch_df.activity_type_id.cast(IntegerType()),
                    batch_df.version_start,
                    batch_df.version_end
                )
            
            activityfact_table = self._delta.forPath(self._spark, table_path)
            activityfact_table.alias("tgt").merge(
                new_activityfacts_df.alias("src"),
                "tgt.activity_id = src.activity_id AND tgt.version_end = \"9999-12-31 23:59:59\""
            ).whenMatchedUpdate(
                set={
                    "version_end": F.expr("src.version_start - INTERVAL 1 SECOND")
                }
            ).execute()

            new_activityfacts_df.write \
                .format("delta") \
                .mode("append") \
                .save(table_path)


        return (
            df.writeStream
                .format("delta")
                .outputMode("update")
                .queryName(f"{tenant_name}-{product_name}-gold-activityfact-stream")
                .option("checkpointLocation", checkpoint_path)
                .foreachBatch(process_batch)
                .start(table_path)
        )

    def write_stream_taskfact(self, df: DataFrame, tenant_name: str, product_name: str) -> StreamingQuery:
        table_path, checkpoint_path = self._build_table_paths(tenant_name, product_name, "gold", "taskfact")

        def process_batch(batch_df: DataFrame, batch_id):
            logger.info(f"Processing batchId: {batch_id}")

            batch_dt = datetime.utcnow()

            workerdim_table_path, _ = self._build_table_paths(tenant_name, product_name, "gold", "workerdimension")
            worker_refs_df = batch_df.select(batch_df.worker_reference).distinct()
            worker_dims_df = self._sync_dimension_table(
                new_dims_df=worker_refs_df,
                dimension_table_path=workerdim_table_path,
                source_col="worker_reference",
                target_col="worker_reference"
            )

            taskdim_table_path, _ = self._build_table_paths(tenant_name, product_name, "gold", "taskdimension")
            task_refs_df = batch_df.select(batch_df.task_reference).distinct()
            task_dims_df = self._sync_dimension_table(
                new_dims_df=task_refs_df,
                dimension_table_path=taskdim_table_path,
                source_col="task_reference",
                target_col="task_reference"
            )

            shiftdim_table_path, _ = self._build_table_paths(tenant_name, product_name, "gold", "shiftdimension")
            shift_refs_df = batch_df.select(batch_df.shift_reference).distinct()
            shift_dims_df = self._sync_dimension_table(
                new_dims_df=shift_refs_df,
                dimension_table_path=shiftdim_table_path,
                source_col="shift_reference",
                target_col="shift_reference"
            )

            batch_df = batch_df \
                .withColumn("version_start", F.lit(batch_dt)) \
                .withColumn("version_end", F.lit("9999-12-31 23:59:59").cast(TimestampType())) 

            new_taskfacts_df = batch_df \
                .join(worker_dims_df, "worker_reference", "left") \
                .join(task_dims_df, "task_reference", "left") \
                .join(shift_dims_df, "shift_reference", "left") \
                .select(
                    task_dims_df.id.cast(IntegerType()).alias("task_id"),
                    worker_dims_df.id.cast(IntegerType()).alias("worker_id"),
                    shift_dims_df.id.cast(IntegerType()).alias("shift_id"),
                    batch_df.task_created_datetime,
                    batch_df.task_created_datetime_utc,
                    batch_df.task_start_datetime,
                    batch_df.task_start_datetime_utc,
                    batch_df.task_end_datetime,
                    batch_df.task_end_datetime_utc,               
                    batch_df.planned_task_start_datetime,
                    batch_df.planned_task_start_datetime_utc,
                    batch_df.planned_task_end_datetime,
                    batch_df.planned_task_end_datetime_utc,
                    batch_df.version_start,
                    batch_df.version_end
                )
            
            taskfact_table = self._delta.forPath(self._spark, table_path)
            taskfact_table.alias("tgt").merge(
                new_taskfacts_df.alias("src"),
                "tgt.task_id = src.task_id AND tgt.version_end = \"9999-12-31 23:59:59\""
            ).whenMatchedUpdate(
                set={
                    "version_end": F.expr("src.version_start - INTERVAL 1 SECOND")
                }
            ).execute()

            new_taskfacts_df.write \
                .format("delta") \
                .mode("append") \
                .save(table_path)

        return (
            df.writeStream
                .format("delta")
                .outputMode("update")
                .queryName(f"{tenant_name}-{product_name}-gold-taskfact-stream")
                .option("checkpointLocation", checkpoint_path)
                .foreachBatch(process_batch)
                .start(table_path)
        )
    
    def write_stream_taskstatusfact_milestones(self, df: DataFrame, tenant_name: str, product_name: str) -> StreamingQuery:
        table_path, checkpoint_path = self._build_table_paths(tenant_name, product_name, "gold", "taskstatusfact")
        checkpoint_path = checkpoint_path + "_milestone" 

        def process_batch(batch_df: DataFrame, batch_id):
            logger.info(f"Processing batchId: {batch_id}")

            batch_dt = datetime.utcnow()

            taskdim_table_path, _ = self._build_table_paths(tenant_name, product_name, "gold", "taskdimension")
            task_refs_df = batch_df.select(batch_df.task_reference).distinct()
            task_dims_df = self._sync_dimension_table(
                new_dims_df=task_refs_df,
                dimension_table_path=taskdim_table_path,
                source_col="task_reference",
                target_col="task_reference"
            )

            shiftdim_table_path, _ = self._build_table_paths(tenant_name, product_name, "gold", "shiftdimension")
            shift_refs_df = batch_df.select(batch_df.shift_reference).distinct()
            shift_dims_df = self._sync_dimension_table(
                new_dims_df=shift_refs_df,
                dimension_table_path=shiftdim_table_path,
                source_col="shift_reference",
                target_col="shift_reference"
            )

            batch_df = batch_df \
                .withColumn("version_start", F.lit(batch_dt)) \
                .withColumn("version_end", F.lit("9999-12-31 23:59:59").cast(TimestampType())) 

            new_taskstatusfacts_df = batch_df \
                .join(task_dims_df, "task_reference", "left") \
                .join(shift_dims_df, "shift_reference", "left") \
                .select(
                    task_dims_df.id.cast(IntegerType()).alias("task_id"),
                    shift_dims_df.id.cast(IntegerType()).alias("shift_id"),
                    batch_df.status_id,
                    batch_df.status_start_datetime,
                    batch_df.status_start_datetime_utc,
                    batch_df.status_end_datetime,
                    batch_df.status_end_datetime_utc,
                    batch_df.version_start,
                    batch_df.version_end
                )
            
            taskstatusfact_table = self._delta.forPath(self._spark, table_path)
            taskstatusfact_table.alias("tgt").merge(
                new_taskstatusfacts_df.alias("src"),
                "src.task_id = tgt.task_id AND src.status_id = tgt.status_id AND tgt.version_end = \"9999-12-31 23:59:59\""
            ).whenMatchedUpdate(
                set={
                    "version_end": F.expr("src.version_start - INTERVAL 1 SECOND")
                }
            ).execute()

            new_taskstatusfacts_df.write \
                .format("delta") \
                .mode("append") \
                .save(table_path)

        return (
            df.writeStream
                .format("delta")
                .outputMode("update")
                .queryName(f"{tenant_name}-{product_name}-gold-taskstatusfact-milestones-stream")
                .option("checkpointLocation", checkpoint_path)
                .foreachBatch(process_batch)
                .start(table_path)
        )
    
    def write_stream_taskgeoroutefact(self, df: DataFrame, tenant_name: str, product_name: str) -> StreamingQuery:
        target_table_path, checkpoint_path = self._build_table_paths(tenant_name, product_name, "gold", "taskgeoroutefact") 

        def process_batch(batch_df: DataFrame, batch_id):
            logger.info(f"Processing batchId: {batch_id}")

            batch_dt = datetime.utcnow()

            activityfact_table_path, _ = self._build_table_paths(
                tenant_name=tenant_name,
                product_name=product_name,
                data_layer="gold",
                table_name="activityfact"
            )
            activityfact_df = self._spark.read.format("delta").load(activityfact_table_path)
            activityfact_df = activityfact_df.filter(activityfact_df.version_end == '9999-12-31 23:59:59') 

            batch_df = batch_df \
                .select(batch_df.shift_id.alias("incoming_shift_id")) \
                .distinct()
            
            joined_df = activityfact_df \
                .join(batch_df, batch_df.incoming_shift_id == activityfact_df.shift_id) 
            
            window_spec_task = Window.partitionBy("task_id").orderBy(F.col("activity_start_datetime_utc").desc())

            joined_df = joined_df \
                .withColumn("rank", F.row_number().over(window_spec_task)) \
                .filter(F.col("rank") == 1) \
                .drop("rank")

            window_spec_shift = Window.partitionBy("shift_id").orderBy("activity_start_datetime_utc")
            
            result_df = joined_df \
                .withColumn("task_order", F.row_number().over(window_spec_shift)) \
                .withColumn("task_target_latitude", F.lead("activity_end_latitude", 1).over(window_spec_shift)) \
                .withColumn("task_target_longitude", F.lead("activity_end_longitude", 1).over(window_spec_shift)) \
                .withColumn("version_start", F.lit(batch_dt)) \
                .withColumn("version_end", F.lit("9999-12-31 23:59:59").cast(TimestampType())) 
        
            result_df = result_df \
                .select(
                    result_df.shift_id,
                    result_df.task_id,
                    result_df.activity_end_latitude.alias("task_source_latitude"),
                    result_df.activity_end_longitude.alias("task_source_longitude"),
                    result_df.planned_activity_start_datetime_utc.alias("planned_start_datetime_utc"),
                    result_df.activity_type_id.alias("task_status_id"),
                    result_df.task_target_latitude,
                    result_df.task_target_longitude,
                    result_df.task_order,
                    result_df.version_start,
                    result_df.version_end
                )
                      
            taskgeoroute_tbl = self._delta.forPath(self._spark, target_table_path)

            taskgeoroute_tbl.alias("tgt").merge(
                batch_df.alias("src"),
                "tgt.shift_id = src.incoming_shift_id AND tgt.version_end = '9999-12-31 23:59:59'"
            ).whenMatchedUpdate(
                set={
                    "version_end": "CURRENT_TIMESTAMP() - INTERVAL 1 SECOND"
                }
            ).execute()

            result_df.write \
                .format("delta") \
                .mode("append") \
                .save(target_table_path)

        return (
            df.writeStream
                .format("delta")
                .outputMode("update")
                .queryName(f"{tenant_name}-{product_name}-gold-taskgeoroutefact-stream")
                .option("checkpointLocation", checkpoint_path)
                .foreachBatch(process_batch)
                .start()
         )
    
    
    def write_stream_taskstatusfact_activities(self, df: DataFrame, tenant_name: str, product_name: str) -> StreamingQuery:
        table_path, checkpoint_path = self._build_table_paths(tenant_name, product_name, "gold", "taskstatusfact")
        checkpoint_path = checkpoint_path + "_activity" 

        def process_batch(batch_df: DataFrame, batch_id):
            logger.info(f"Processing batchId: {batch_id}")

            batch_dt = datetime.utcnow()

            taskdim_table_path, _ = self._build_table_paths(tenant_name, product_name, "gold", "taskdimension")
            task_refs_df = batch_df.select(batch_df.task_reference).distinct()
            task_dims_df = self._sync_dimension_table(
                new_dims_df=task_refs_df,
                dimension_table_path=taskdim_table_path,
                source_col="task_reference",
                target_col="task_reference"
            )

            shiftdim_table_path, _ = self._build_table_paths(tenant_name, product_name, "gold", "shiftdimension")
            shift_refs_df = batch_df.select(batch_df.shift_reference).distinct()
            shift_dims_df = self._sync_dimension_table(
                new_dims_df=shift_refs_df,
                dimension_table_path=shiftdim_table_path,
                source_col="shift_reference",
                target_col="shift_reference"
            )

            batch_df = batch_df \
                .withColumn("version_start", F.lit(batch_dt)) \
                .withColumn("version_end", F.lit("9999-12-31 23:59:59").cast(TimestampType())) 

            new_taskstatusfacts_df = batch_df \
                .join(task_dims_df, "task_reference", "left") \
                .join(shift_dims_df, "shift_reference", "left") \
                .select(
                    task_dims_df.id.cast(IntegerType()).alias("task_id"),
                    shift_dims_df.id.cast(IntegerType()).alias("shift_id"),
                    batch_df.status_id,
                    batch_df.status_start_datetime,
                    batch_df.status_start_datetime_utc,
                    batch_df.status_end_datetime,
                    batch_df.status_end_datetime_utc,
                    batch_df.version_start,
                    batch_df.version_end
                )
            
            taskstatusfact_table = self._delta.forPath(self._spark, table_path)
            taskstatusfact_table.alias("tgt").merge(
                new_taskstatusfacts_df.alias("src"),
                "src.task_id = tgt.task_id AND src.status_id = tgt.status_id AND tgt.version_end = \"9999-12-31 23:59:59\""
            ).whenMatchedUpdate(
                set={
                    "version_end": F.expr("src.version_start - INTERVAL 1 SECOND")
                }
            ).execute()

            new_taskstatusfacts_df.write \
                .format("delta") \
                .mode("append") \
                .save(table_path)

        return (
            df.writeStream
                .format("delta")
                .outputMode("update")
                .queryName(f"{tenant_name}-{product_name}-gold-taskstatusfact-activities-stream")
                .option("checkpointLocation", checkpoint_path)
                .foreachBatch(process_batch)
                .start(table_path)
        )

    def get_changed_shifts_ids(self, tenant_name: str, product_name: str) -> DataFrame:
        shiftfacts_table_path, _ = self._build_table_paths(tenant_name, product_name, "gold", "shiftfact")
        shiftkpifacts_table_path, _ = self._build_table_paths(tenant_name, product_name, "gold", "shiftkpifact")

        shiftkpifacts_df = self._spark.read.format("delta") \
            .load(shiftkpifacts_table_path)

        # get the last time the batch was run for shiftkpifact (usually 5 minutes ago)
        last_batch_dt = shiftkpifacts_df.select(
            F.coalesce(F.max(shiftkpifacts_df.version_start), F.lit("1900-01-01 00:00:00").cast(TimestampType())).alias("max_version_start")
        ).collect()[0]["max_version_start"]

        logger.info(f"Last batch run datetime: {last_batch_dt}")

        if last_batch_dt < datetime.utcnow() - timedelta(hours=SHIFT_KPI_FACT_MAX_HOURS):
            last_batch_dt = datetime.utcnow() - timedelta(hours=SHIFT_KPI_FACT_MAX_HOURS)
        
        logger.info(f"Using {last_batch_dt} as shiftkpifact horizon")

        shiftfacts_df = self._spark.read.format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingTimestamp", last_batch_dt) \
            .load(shiftfacts_table_path)

        # get the shift_ids of all shiftfacts that are new/updated since the last batch run time
        new_shiftfacts_df = shiftfacts_df \
            .select(shiftfacts_df.shift_id) \
            .distinct()
        
        ################ activityfact shift_id's ############################

        activityfacts_table_path, _ = self._build_table_paths(tenant_name, product_name, "gold", "activityfact")
        activityfacts_df = self._spark.read.format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingTimestamp", last_batch_dt) \
            .load(activityfacts_table_path)              
        
        new_activityfacts_df = activityfacts_df \
            .select(activityfacts_df.shift_id) \
            .distinct()

        ################ taskfact shift_id's ############################

        taskfacts_table_path, _ = self._build_table_paths(tenant_name, product_name, "gold", "taskfact")
        taskfacts_df = self._spark.read.format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingTimestamp", last_batch_dt) \
            .load(taskfacts_table_path)  
        
        new_taskfacts_df = taskfacts_df \
            .select(taskfacts_df.shift_id) \
            .distinct()
        
        ################ taskstatusfact shift_id's ############################

        taskstatusfacts_table_path, _ = self._build_table_paths(tenant_name, product_name, "gold", "taskstatusfact")
        taskstatusfacts_df = self._spark.read.format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingTimestamp", last_batch_dt) \
            .load(taskstatusfacts_table_path)  
        
        new_taskstatusfacts_df = taskstatusfacts_df \
            .select(taskstatusfacts_df.shift_id) \
            .distinct()

        # union all source tables and return the unique shift id's
        return (new_shiftfacts_df \
                .union(new_activityfacts_df) \
                .union(new_taskfacts_df) \
                .union(new_taskstatusfacts_df) \
                .select(F.col("shift_id")) \
                .distinct())
    

    def get_shiftfact_batch(self, changed_shifts_ids_df: DataFrame, tenant_name: str, product_name: str) -> DataFrame:
        shiftfact_table_path, _ = self._build_table_paths(tenant_name, product_name, "gold", "shiftfact")
        shiftfact_df = self._spark.read.format("delta").load(shiftfact_table_path)

        # adding only the latest versions of shifts which have changes since the last batch run to the current batch.
        latest_shiftfact_df = shiftfact_df \
            .filter(shiftfact_df.version_end == '9999-12-31 23:59:59') \
            .join(changed_shifts_ids_df, "shift_id", "inner") \
            .select(
                shiftfact_df.shift_id,
                shiftfact_df.worker_id,
                shiftfact_df.shift_start_date_id,
                shiftfact_df.shift_start_datetime,
                shiftfact_df.shift_start_datetime_utc,
                shiftfact_df.shift_end_datetime,
                shiftfact_df.shift_end_datetime_utc,
                shiftfact_df.paid_minutes
            )
        
        return latest_shiftfact_df
    
    def get_activityfact_batch(self, changed_shifts_ids_df: DataFrame, tenant_name: str, product_name: str) -> DataFrame:
        activityfact_table_path, _ = self._build_table_paths(tenant_name, product_name, "gold", "activityfact")
        activityfact_df = self._spark.read.format("delta").load(activityfact_table_path)

        activitytypedim_table_path, _ = self._build_table_paths(tenant_name, product_name, "gold", "activitytypedimension")
        activitytypedimension_df = self._spark.read.format("delta").load(activitytypedim_table_path)

        # adding only the latest versions of activities which have changed since last batch run
        latest_activityfact_df = activityfact_df \
            .filter(activityfact_df.version_end == '9999-12-31 23:59:59') \
            .join(changed_shifts_ids_df, "shift_id", "inner") \
            .join(activitytypedimension_df, activityfact_df.activity_type_id == activitytypedimension_df.id) \
            .select(
                activityfact_df.activity_id,
                activityfact_df.shift_id,
                activityfact_df.worker_id,
                activityfact_df.task_id,
                activityfact_df.activity_start_datetime,
                activityfact_df.activity_start_datetime_utc,
                activityfact_df.activity_start_latitude,
                activityfact_df.activity_start_longitude,
                activityfact_df.activity_start_location_validity,
                activityfact_df.activity_end_datetime,
                activityfact_df.activity_end_datetime_utc,
                activityfact_df.activity_end_latitude,
                activityfact_df.activity_end_longitude,
                activityfact_df.activity_end_location_validity,
                activityfact_df.planned_activity_start_datetime_utc,
                activityfact_df.planned_activity_start_latitude,
                activityfact_df.planned_activity_start_longitude,
                activityfact_df.planned_activity_start_location_validity,
                activityfact_df.planned_activity_end_datetime_utc,
                activityfact_df.planned_activity_end_latitude,
                activityfact_df.planned_activity_end_longitude,
                activityfact_df.planned_activity_end_location_validity,
                activitytypedimension_df.activity_type,
                activitytypedimension_df.productivity_type
            )
        
        return latest_activityfact_df
    
    def get_taskfact_batch(self, changed_shifts_ids_df: DataFrame, tenant_name: str, product_name: str) -> DataFrame:
        taskfact_table_path, _ = self._build_table_paths(tenant_name, product_name, "gold", "taskfact")
        taskfact_df = self._spark.read.format("delta").load(taskfact_table_path)

        # adding only the latest versions of tasks which have changed since last batch run
        latest_taskfact_df = taskfact_df \
            .filter(taskfact_df.version_end == '9999-12-31 23:59:59') \
            .join(changed_shifts_ids_df, "shift_id", "inner") \
            .select(
                taskfact_df.task_id,
                taskfact_df.worker_id,
                taskfact_df.shift_id,
                taskfact_df.task_created_datetime_utc,
                taskfact_df.task_start_datetime_utc,
                taskfact_df.task_end_datetime_utc,                 
                taskfact_df.planned_task_start_datetime_utc,
                taskfact_df.planned_task_end_datetime_utc
            )
        
        return latest_taskfact_df
    
    def get_taskstatusfact_batch(self, changed_shifts_ids_df: DataFrame, tenant_name: str, product_name: str) -> DataFrame:
        taskstatusfact_table_path, _ = self._build_table_paths(tenant_name, product_name, "gold", "taskstatusfact")
        taskstatusfact_df = self._spark.read.format("delta").load(taskstatusfact_table_path)

        taskstatusdim_table_path, _ = self._build_table_paths(tenant_name, product_name, "gold", "taskstatusdimension")
        taskstatusdim_df = self._spark.read.format("delta").load(taskstatusdim_table_path)

        # adding only the latest versions of task statuses for tasks which have changed since last batch run
        latest_taskstatusfact_df = taskstatusfact_df \
            .filter(taskstatusfact_df.version_end == '9999-12-31 23:59:59') \
            .join(taskstatusdim_df, taskstatusfact_df.status_id == taskstatusdim_df.id) \
            .join(changed_shifts_ids_df, "shift_id", "inner") \
            .select(
                taskstatusfact_df.task_id,
                taskstatusfact_df.shift_id,
                taskstatusfact_df.status_start_datetime_utc,
                taskstatusfact_df.status_end_datetime_utc,
                taskstatusdim_df.status,
                taskstatusdim_df.detailed_status
            )
        
        return latest_taskstatusfact_df
        

    def write_shiftkpifact(self, pddf: pd.DataFrame, batch_dt: datetime, tenant_name: str, product_name: str) -> None:
        table_path, _ = self._build_table_paths(tenant_name, product_name, "gold", "shiftkpifact")
        shiftkpifact_table = self._delta.forPath(self._spark, table_path)

        ## update existing versions
        df = self._spark.createDataFrame(pddf)
        df = (
            df
                .select(
                    df.shift_id.cast(IntegerType()),
                    df.worker_id.cast(IntegerType()),
                    df.shift_start_date_id.cast(IntegerType()),
                    df.kpi_id.cast(IntegerType()),
                    df.kpi_value.cast(FloatType()),
                    df.kpi_norm_value.cast(DecimalType(5,2))
                )
                .withColumn("version_start", F.lit(batch_dt)) \
                .withColumn("version_end", F.lit("9999-12-31 23:59:59").cast(TimestampType()))
        )
        shiftkpifact_table.alias("tgt").merge(
            df.alias("src"),
            """
                    tgt.shift_id = src.shift_id 
                AND tgt.kpi_id = src.kpi_id
                AND tgt.version_end = \"9999-12-31 23:59:59\"
            """
        ).whenMatchedUpdate(
            set={
                "version_end": F.expr("src.version_start - INTERVAL 1 SECOND")
            }
        ).execute()

        # write new versions
        df.write \
            .format("delta") \
            .mode("append") \
            .save(table_path)

    
    def write_stream_shiftkpafact(self, df: DataFrame, tenant_name: str, product_name: str) -> None:
        table_path, checkpoint_path = self._build_table_paths(
            tenant_name,
            product_name,
            "gold",
            "shiftkpafact"
        )

        def process_batch(batch_df: DataFrame, batch_id):
            logger.info(f"Processing batch {batch_id}")
            batch_dt = datetime.utcnow()

            skf_tbl_df = self._delta.forName(self._spark, self.SHIFTKPIFACT_TABLE_NAME.substitute(tenant_name=tenant_name, product_name=product_name)).toDF()
            
            dist_shift_ids = batch_df.select(batch_df.shift_id).distinct()

            filtered_df = (
                skf_tbl_df
                    .filter(skf_tbl_df.version_end == '9999-12-31 23:59:59') # only look at most recent version
                    .join(dist_shift_ids, "shift_id", "inner") # filter in on shifts in this micro-batch only
                    .select(
                        skf_tbl_df.shift_id,
                        skf_tbl_df.worker_id,
                        skf_tbl_df.shift_start_date_id,
                        skf_tbl_df.kpi_id,
                        skf_tbl_df.kpi_value,
                        skf_tbl_df.kpi_norm_value
                    )
            )

            kpidim_df = self._delta.forName(self._spark, self.KPI_DIMENSION_TABLE_NAME.substitute(tenant_name=tenant_name, product_name=product_name)).toDF()

            agg_df = (
                filtered_df 
                    .join(kpidim_df, kpidim_df.id == filtered_df.kpi_id, "inner")
                    .groupBy(filtered_df.shift_id, kpidim_df.kpa_id)
                    .agg(
                        F.first(filtered_df.worker_id).alias("worker_id"),
                        F.first(filtered_df.shift_start_date_id).alias("shift_start_date_id"),
                        F.avg(filtered_df.kpi_value).alias("avg_kpi_value"),
                        F.avg(filtered_df.kpi_norm_value).alias("avg_kpi_norm_value")
                    )
            )

            res_df = agg_df.select(
                agg_df.shift_id,
                agg_df.worker_id,
                agg_df.shift_start_date_id,
                agg_df.kpa_id,
                agg_df.avg_kpi_value.cast(FloatType()),
                agg_df.avg_kpi_norm_value.cast(DecimalType(5,2))
            )

            res_df = res_df \
                .withColumn("version_start", F.lit(batch_dt)) \
                .withColumn("version_end", F.lit("9999-12-31 23:59:59").cast(TimestampType()))
            
            # update existing versions
            shiftkpafact_table = self._delta.forPath(self._spark, table_path)
            shiftkpafact_table.alias("tgt").merge(
                res_df.alias("src"),
                "tgt.shift_id = src.shift_id AND tgt.kpa_id = src.kpa_id AND tgt.version_end = \"9999-12-31 23:59:59\""
            ).whenMatchedUpdate(
                set={
                    "version_end": F.expr("src.version_start - INTERVAL 1 SECOND")
                }
            ).execute()

            # add new versions
            res_df.write \
                .format("delta") \
                .mode("append") \
                .save(table_path)


        df.writeStream \
            .format("delta") \
            .outputMode("update") \
            .queryName(f"{tenant_name}-{product_name}-gold-shiftkpafact-stream") \
            .option("checkpointLocation", checkpoint_path) \
            .trigger(processingTime="30 seconds") \
            .foreachBatch(process_batch) \
            .start(table_path)


    def write_stream_shiftkpioverallfact(self, df: DataFrame, tenant_name: str, product_name: str) -> None:
        table_path, checkpoint_path = self._build_table_paths(
            tenant_name,
            product_name,
            "gold",
            "shiftkpioverallfact"
        )

        def process_batch(batch_df: DataFrame, batch_id):
            logger.info(f"Processing batch {batch_id}")

            batch_dt = datetime.utcnow()

            skf_tbl_df = self._delta.forName(self._spark, self.SHIFTKPIFACT_TABLE_NAME.substitute(tenant_name=tenant_name, product_name=product_name)).toDF()
            
            dist_shift_ids = batch_df.select(batch_df.shift_id).distinct()

            filtered_df = (
                skf_tbl_df
                    .filter(skf_tbl_df.version_end == '9999-12-31 23:59:59')
                    .join(dist_shift_ids, "shift_id", "inner")
                    .select(
                        skf_tbl_df.shift_id,
                        skf_tbl_df.worker_id,
                        skf_tbl_df.shift_start_date_id,
                        skf_tbl_df.kpi_value,
                        skf_tbl_df.kpi_norm_value
                    )
            )

            agg_df = (
                filtered_df
                    .groupBy(filtered_df.shift_id)
                    .agg(
                        F.first(filtered_df.worker_id).alias("worker_id"),
                        F.first(filtered_df.shift_start_date_id).alias("shift_start_date_id"),
                        F.avg(filtered_df.kpi_value).alias("avg_kpi_value"),
                        F.avg(filtered_df.kpi_norm_value).alias("avg_kpi_norm_value")
                    )
            )

            res_df = agg_df.select(
                agg_df.shift_id,
                agg_df.worker_id,
                agg_df.shift_start_date_id,
                agg_df.avg_kpi_value.cast(FloatType()),
                agg_df.avg_kpi_norm_value.cast(DecimalType(5,2))
            )

            res_df = res_df \
                .withColumn("version_start", F.lit(batch_dt)) \
                .withColumn("version_end", F.lit("9999-12-31 23:59:59").cast(TimestampType()))

            # update existing versions
            table = self._delta.forPath(self._spark, table_path)
            table.alias("tgt").merge(
                res_df.alias("src"),
                "tgt.shift_id = src.shift_id AND tgt.version_end = \"9999-12-31 23:59:59\""
            ).whenMatchedUpdate(
                set={
                    "version_end": F.expr("src.version_start - INTERVAL 1 SECOND")
                }
            ).execute()

            # write new versions
            res_df.write \
                .format("delta") \
                .mode("append") \
                .save(table_path)

        
        df.writeStream \
            .format("delta") \
            .outputMode("update") \
            .queryName(f"{tenant_name}-{product_name}-gold-shiftkpioverallfact-stream") \
            .option("checkpointLocation", checkpoint_path) \
            .trigger(processingTime="30 seconds") \
            .foreachBatch(process_batch) \
            .start(table_path)
        
    def write_stream_shiftkpaoverallfact(self, df: DataFrame, tenant_name: str, product_name: str) -> None:
        table_path, checkpoint_path = self._build_table_paths(
            tenant_name,
            product_name,
            "gold",
            "shiftkpaoverallfact"
        )

        def process_batch(batch_df: DataFrame, batch_id):
            logger.info(f"Processing batch {batch_id}")

            batch_dt = datetime.utcnow()

            skf_tbl_df = self._delta.forName(self._spark, self.SHIFTKPAFACT_TABLE_NAME.substitute(tenant_name=tenant_name, product_name=product_name)).toDF()
            
            dist_shift_ids = batch_df.select(batch_df.shift_id).distinct()

            filtered_df = (
                skf_tbl_df
                    .filter(skf_tbl_df.version_end == '9999-12-31 23:59:59')
                    .join(dist_shift_ids, "shift_id", "inner")
                    .select(
                        skf_tbl_df.shift_id,
                        skf_tbl_df.worker_id,
                        skf_tbl_df.shift_start_date_id,
                        skf_tbl_df.avg_kpi_value,
                        skf_tbl_df.avg_kpi_norm_value
                    )
            )

            agg_df = (
                filtered_df
                    .groupBy(filtered_df.shift_id)
                    .agg(
                        F.first(filtered_df.worker_id).alias("worker_id"),
                        F.first(filtered_df.shift_start_date_id).alias("shift_start_date_id"),
                        F.avg(filtered_df.avg_kpi_value).alias("avg_kpa_value"),
                        F.avg(filtered_df.avg_kpi_norm_value).alias("avg_kpa_norm_value")
                    )
            )

            res_df = agg_df.select(
                agg_df.shift_id,
                agg_df.worker_id,
                agg_df.shift_start_date_id,
                agg_df.avg_kpa_value.cast(FloatType()),
                agg_df.avg_kpa_norm_value.cast(DecimalType(5,2))
            )

            res_df = res_df \
                .withColumn("version_start", F.lit(batch_dt)) \
                .withColumn("version_end", F.lit("9999-12-31 23:59:59").cast(TimestampType()))

            # update existing versions
            table = self._delta.forPath(self._spark, table_path)
            table.alias("tgt").merge(
                res_df.alias("src"),
                "tgt.shift_id = src.shift_id AND tgt.version_end = \"9999-12-31 23:59:59\""
            ).whenMatchedUpdate(
                set={
                    "version_end": F.expr("src.version_start - INTERVAL 1 SECOND")
                }
            ).execute()

            # write new versions
            res_df.write \
                .format("delta") \
                .mode("append") \
                .save(table_path)

        
        df.writeStream \
            .format("delta") \
            .outputMode("update") \
            .queryName(f"{tenant_name}-{product_name}-gold-shiftkpaoverallfact-stream") \
            .option("checkpointLocation", checkpoint_path) \
            .trigger(processingTime="30 seconds") \
            .foreachBatch(process_batch) \
            .start(table_path)
        

    def write_shiftexclusionfact(self, excluded_df: DataFrame, unexcluded_df: DataFrame, tenant_name: str, product_name: str) -> None:
        table_path, _ = self._build_table_paths(tenant_name, product_name, "gold", "shiftexclusionfact")
        shift_dim_table_path, _ = self._build_table_paths(tenant_name, product_name, "gold", "shiftdimension")

        shiftexclusionfact_table = self._delta.forPath(self._spark, table_path)

        shifts_ref_df = excluded_df.select("shift_reference").distinct()

        shift_dims_df = self._sync_dimension_table(
                new_dims_df=shifts_ref_df,
                dimension_table_path=shift_dim_table_path,
                source_col="shift_reference",
                target_col="shift_reference"
            )
        
        excluded_df = excluded_df.join(shift_dims_df, "shift_reference", "inner") \
            .select(
                shift_dims_df.id.alias("shift_id").cast(LongType()),
                excluded_df.exclusion_reason_id,
                excluded_df.version_start,
                excluded_df.version_end
            )

        ## Update existing records and insert new excluded shifts
        shiftexclusionfact_table.alias("tgt").merge(
            excluded_df.alias("src"),
            "tgt.shift_id = src.shift_id AND tgt.exclusion_reason_id = src.exclusion_reason_id AND tgt.version_end = \"9999-12-31 23:59:59\""
        ).whenMatchedUpdate(
            set={
                "version_end": F.expr("src.version_start - INTERVAL 1 SECOND") 
            }
        ).execute()

        excluded_df.write \
            .format("delta") \
            .mode("append") \
            .save(table_path)

        unexcluded_df = unexcluded_df \
            .join(shift_dims_df, "shift_reference", "inner") \
            .select(
                shift_dims_df.id.alias("shift_id").cast(IntegerType()),
                unexcluded_df.version_end
            )
        
        ## Update records in case a given shift is no longer excluded
        shiftexclusionfact_table.alias("tgt").merge(
            unexcluded_df.alias("src"),
            "tgt.shift_id = src.shift_id AND tgt.version_end = \"9999-12-31 23:59:59\""
        ).whenMatchedUpdate(
            set={
                "version_end":  "src.version_end"
            }
        ).execute()

    def write_stream_tasklateststatusfact(self, df: DataFrame, tenant_name: str, product_name: str) -> None:
        target_table_path, checkpoint_path = self._build_table_paths(tenant_name, product_name, "gold", "tasklateststatusfact")

        def process_batch(batch_df: DataFrame, batch_id):
            logger.info(f"Processing batch {batch_id}")

            batch_dt = datetime.utcnow()

            batch_df = batch_df \
                .select(batch_df.shift_id.alias("incoming_shift_id")) \
                .distinct()
            
            taskstatusfact_table_path, _ = self._build_table_paths(tenant_name, product_name, "gold", "taskstatusfact")
            taskstatusfact_df = self._spark.read.format("delta").load(taskstatusfact_table_path)  

            joined_df = taskstatusfact_df \
                .join(batch_df, 
                    (taskstatusfact_df.shift_id == batch_df.incoming_shift_id) & 
                    (taskstatusfact_df.version_end == '9999-12-31 23:59:59'))

            window_spec_task = Window.partitionBy("shift_id", "task_id").orderBy(F.desc("status_start_datetime_utc"))
            window_spec_shift = Window.partitionBy("shift_id").orderBy(F.desc("status_start_datetime_utc"))

            latest_status_df = (
                joined_df.withColumn("rank", F.row_number().over(window_spec_task))
                    .filter(F.col("rank") == 1)
                    .drop("rank")
            )

            result_df = latest_status_df \
                .withColumn("task_order_desc", F.row_number().over(window_spec_shift)) \
                .withColumn("version_start", F.lit(batch_dt)) \
                .withColumn("version_end", F.lit("9999-12-31 23:59:59").cast(TimestampType())) \
                .select(
                    F.col("shift_id"),
                    F.col("task_id"),
                    F.col("status_id").alias("latest_status_id"),
                    F.col("status_start_datetime").alias("latest_status_start_datetime"),
                    F.col("status_start_datetime_utc").alias("latest_status_start_datetime_utc"),
                    F.col("status_end_datetime").alias("latest_status_end_datetime"),
                    F.col("status_end_datetime_utc").alias("latest_status_end_datetime_utc"),
                    F.col("task_order_desc"),
                    F.col("version_start"), 
                    F.col("version_end") 
                )
            
            target = self._delta.forPath(self._spark, target_table_path)
            target.alias("tgt").merge(
                batch_df.alias("src"),
                "tgt.shift_id = src.incoming_shift_id AND tgt.version_end = \"9999-12-31 23:59:59\""
            ).whenMatchedUpdate(
                set={
                    "version_end": "CURRENT_TIMESTAMP() - INTERVAL 1 SECOND",
                }
            ).execute()

            result_df.write \
                .format("delta") \
                .mode("append") \
                .save(target_table_path)
        
        df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .queryName(f"{tenant_name}-{product_name}-gold-tasklateststatusfact-stream") \
            .option("checkpointLocation", checkpoint_path) \
            .trigger(processingTime="30 seconds") \
            .foreachBatch(process_batch) \
            .start(target_table_path)

def new_repository() -> Repository:
    builder: SparkSession.Builder = SparkSession.builder
    spark_sess: SparkSession = builder.appName(SPARK_SESSION_APP_NAME).getOrCreate()

    return Repository(spark_sess=spark_sess, delta=DeltaTable)