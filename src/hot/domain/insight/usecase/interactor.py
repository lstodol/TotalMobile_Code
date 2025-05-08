import pyspark.sql.functions as F
import logging
import duckdb
import pandas as pd
from datetime import datetime
from functools import reduce
from string import Template
from usecase.interactor_interface import InteractorInterface
from storage.repository_interface import RepositoryInterface
from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import *
from domain import domain, activity
from domain.domain import timed_execution

logger = logging.getLogger(__name__)


class Interactor(InteractorInterface):
    def __init__(self, repository: RepositoryInterface) -> None:
        super().__init__()
        self._repository: RepositoryInterface = repository

    def _add_ingestion_date(self, input_df: DataFrame):
        output_df = input_df.withColumn("load_date", F.current_timestamp())
        return output_df
    
    def _remove_duplicates(self, input_df: DataFrame):
        output_df = input_df.dropDuplicates()
        return output_df
    

    def _cdf_filter(self, df: DataFrame) -> DataFrame:
        return df.filter(df["_change_type"].isin("insert", "update_postimage"))
    
    def _add_time_id(self, df: DataFrame, colName: str, srcColName: str):
        def _calc_time_id(dt: datetime):
            if dt is None:
                return None
            return dt.hour * 3600 + dt.minute * 60 + dt.second
        
        calc_time_id_udf = F.udf(_calc_time_id, IntegerType())

        return df.withColumn(colName, calc_time_id_udf(df[srcColName]))

    def load_silver_shift_table(self, tenant_name: str, product_name: str) -> None:            
        logger.info("Initialising shift silver table stream")
            
        activity_df = self._repository.read_stream(
            source_table_name="activity",
            data_layer="bronze", 
            tenant_name=tenant_name, 
            product_name=product_name
        )

        # Shifts MUST not have parent activity reference & HAVE type = 0
        activity_df = activity_df.filter(
            (activity_df.parent_activity_reference.isNull())
            & (activity_df.type == 0)
        )

        activity_df = self._cdf_filter(activity_df)
        activity_df = self._remove_duplicates(activity_df)
        activity_df = self._add_ingestion_date(activity_df)

        source_df = activity_df.select(
            activity_df.reference.alias("reference"),
            activity_df.resource_reference.alias("worker_reference"),
            activity_df.start_datetime_local,
            F.to_utc_timestamp(activity_df.start_datetime_local, activity_df.timezone).alias("start_datetime_utc"),
            activity_df.finish_datetime_local.alias("end_datetime_local"),
            F.to_utc_timestamp(activity_df.finish_datetime_local, activity_df.timezone).alias("end_datetime_utc"),
            activity_df.load_date
        )
        
        streaming_query = self._repository.write_stream_shift(
            df=source_df, 
            tenant_name=tenant_name, 
            product_name=product_name
        )
            
        logger.info("Completed processing into silver layer of 'shift'")

        streaming_query.awaitTermination()


    def load_silver_activity_table(self, tenant_name: str, product_name: str) -> None:
        logger.info("Initialising activity silver table stream")

        activity_df = self._repository.read_stream("activity", "bronze", tenant_name, product_name)

        activity_df = self._cdf_filter(activity_df)
        activity_df = self._add_ingestion_date(activity_df)

        streaming_query = self._repository.write_stream_activity(activity_df, tenant_name, product_name)

        logger.info("Finished setting up silver layer stream of 'activity'")

        streaming_query.awaitTermination()

    
    def load_silver_task_table(self, tenant_name: str, product_name: str) -> None:
        logger.info("Initialising task silver table stream")

        task_df = self._repository.read_stream("task", "bronze", tenant_name, product_name)
        task_df = self._cdf_filter(task_df)
        task_df = self._add_ingestion_date(task_df)

        task_df = task_df.select(
            task_df.reference,
            F.expr("NULL").alias("resource_reference"),
            F.expr("NULL").alias("allocation_version"),
            task_df.created_datetime_local,
            F.to_utc_timestamp(task_df.created_datetime_local, task_df.timezone).alias("created_datetime_utc"),
            F.expr("NULL").alias("planned_start_datetime_local"),
            F.expr("NULL").alias("planned_start_datetime_utc"),
            F.expr("NULL").alias("outcome_code"),
            F.expr("NULL").alias("outcome_type"),
            task_df.visit_code,
            task_df.visit_count,
            F.expr("NULL").alias("action_type"),
            F.expr("NULL").alias("action_version"),
            task_df.type.alias("task_type"),
            task_df["class"],
            task_df.duration,
            task_df.rank,
            task_df.version.alias("task_version"),
            task_df.load_date
        )

        self._repository.write_stream_task(task_df, tenant_name, product_name)


    def load_silver_task_table_allocations(self, tenant_name: str, product_name: str) -> None:
        logger.info("Initialising task silver table allocations stream")

        alloc_df = self._repository.read_stream("allocation", "bronze", tenant_name, product_name)
        alloc_df = self._cdf_filter(alloc_df)
        alloc_df = self._add_ingestion_date(alloc_df)

        alloc_df = alloc_df.select(
            alloc_df.reference.alias("allocation_reference"),
            alloc_df.task_reference.alias("reference"),
            alloc_df.resource_reference,
            F.expr("NULL").alias("created_datetime_local"),
            F.expr("NULL").alias("created_datetime_utc"),
            alloc_df.planned_start_datetime_local,
            F.to_utc_timestamp(alloc_df.planned_start_datetime_local, alloc_df.timezone).alias("planned_start_datetime_utc"),
            alloc_df.version.alias("allocation_version"),
            F.expr("NULL").alias("outcome_code"),
            F.expr("NULL").alias("outcome_type"),
            F.expr("NULL").alias("visit_code"),
            F.expr("NULL").alias("visit_count"),
            F.expr("NULL").alias("action_type"),
            F.expr("NULL").alias("action_version"),
            F.expr("NULL").alias("task_type"),
            F.expr("NULL").alias("class"),
            F.expr("NULL").alias("duration"),
            F.expr("NULL").alias("rank"),
            F.expr("NULL").alias("task_version"),
            alloc_df.load_date
        )

        self._repository.write_stream_task_allocation(alloc_df, tenant_name, product_name)


    def load_silver_task_table_actions(self, tenant_name: str, product_name: str) -> None:
        logger.info("Initialising task silver table actions stream")

        acts_df = self._repository.read_stream("action", "bronze", tenant_name, product_name)
        acts_df = self._cdf_filter(acts_df)
        acts_df = self._add_ingestion_date(acts_df)

        acts_df = acts_df.select(
            acts_df.reference.alias("action_reference"),
            acts_df.task_reference.alias("reference"),
            F.expr("NULL").alias("resource_reference"),
            F.expr("NULL").alias("allocation_version"),
            F.expr("NULL").alias("created_datetime_local"),
            F.expr("NULL").alias("created_datetime_utc"),
            F.expr("NULL").alias("planned_start_datetime_local"),
            F.expr("NULL").alias("planned_start_datetime_utc"),
            acts_df.outcome_code,
            acts_df.outcome_type,
            F.expr("NULL").alias("visit_code"),
            F.expr("NULL").alias("visit_count"),
            acts_df.type.alias("action_type"),
            acts_df.version.alias("action_version"),
            F.expr("NULL").alias("task_type"),
            F.expr("NULL").alias("class"),
            F.expr("NULL").alias("duration"),
            F.expr("NULL").alias("rank"),
            F.expr("NULL").alias("task_version"),
            acts_df.load_date
        )

        self._repository.write_stream_task_action(acts_df, tenant_name, product_name)


    def load_silver_milestone_table(self, tenant_name: str, product_name: str) -> None:
        logger.info("Initialising milestone silver table actions stream")

        miles_df = self._repository.read_stream("milestone", "bronze", tenant_name, product_name)
        miles_df = self._cdf_filter(miles_df)
        miles_df = self._add_ingestion_date(miles_df)

        miles_df = miles_df.select(
            miles_df.reference,
            miles_df.task_reference,
            miles_df.resource_reference,
            F.when(miles_df.basis == 1, miles_df.created_datetime_local).alias("created_datetime_local"),
            F.when(
                miles_df.basis == 1, 
                F.to_utc_timestamp(miles_df.created_datetime_local, miles_df.timezone)
            ).alias("created_datetime_utc"),
            F.when(miles_df.basis == 0, miles_df.created_datetime_local).alias("planned_datetime_local"),
            F.when(
                miles_df.basis == 0, 
                F.to_utc_timestamp(miles_df.created_datetime_local, miles_df.timezone)
            ).alias("planned_datetime_utc"),
            miles_df.type,
            miles_df.version,
            miles_df.load_date
        )

        self._repository.write_stream_milestone(miles_df, tenant_name, product_name)
    

    def load_gold_shiftfact_table(self, tenant_name: str, product_name: str) -> None:
        logger.info(f"Initialising \"shiftfact\" gold table stream")

        s_df = self._repository.read_stream(
            source_table_name="shift",
            data_layer="silver",
            tenant_name=tenant_name,
            product_name=product_name
        )

        s_df = self._cdf_filter(s_df)

        s_df = s_df \
            .withColumn("shift_start_date_id", F.date_format(s_df.start_datetime_local, "yyyyMMdd")) \
            .withColumn("shift_start_date_utc_id", F.date_format(s_df.start_datetime_utc, "yyyyMMdd")) \
            .withColumn("shift_end_date_id", F.date_format(s_df.end_datetime_local, "yyyyMMdd")) \
            .withColumn("shift_end_date_utc_id", F.date_format(s_df.end_datetime_utc, "yyyyMMdd")) \
            .withColumn("paid_minutes", F.lit(0))

        source_df = s_df.select(
            s_df.reference.alias("shift_reference"),
            s_df.worker_reference.alias("worker_reference"),
            s_df.start_datetime_local.alias("shift_start_datetime"),
            s_df.start_datetime_utc.alias("shift_start_datetime_utc"),
            s_df.shift_start_date_id,
            s_df.shift_start_date_utc_id,
            s_df.end_datetime_local.alias("shift_end_datetime"),
            s_df.end_datetime_utc.alias("shift_end_datetime_utc"),
            s_df.shift_end_date_id,
            s_df.shift_end_date_utc_id,
            s_df.paid_minutes
        )

        streaming_query = self._repository.write_stream_shiftfact(
            df=source_df,
            tenant_name=tenant_name,
            product_name=product_name
        )
            
        logger.info("Completed processing into silver layer of 'shift'")

        streaming_query.awaitTermination()


    def load_gold_activityfact_table(self, tenant_name: str, product_name: str) -> None:
        logger.info("Initialising gold 'activityfact' table stream")

        slvr_act_df = self._repository.read_stream("activity", "silver", tenant_name, product_name)
        slvr_act_df = self._cdf_filter(slvr_act_df)

        activitytypedim_df = self._repository.get_activitytypedimension(tenant_name, product_name)

        slvr_act_df = slvr_act_df \
            .withColumn("activity_start_date_id", F.date_format(slvr_act_df.start_datetime_local, "yyyyMMdd")) \
            .withColumn("activity_start_date_utc_id", F.date_format(slvr_act_df.start_datetime_utc, "yyyyMMdd")) \
            .withColumn("activity_end_date_id", F.date_format(slvr_act_df.end_datetime_local, "yyyyMMdd")) \
            .withColumn("activity_end_date_utc_id", F.date_format(slvr_act_df.end_datetime_utc, "yyyyMMdd")) \
            .withColumn("planned_activity_start_date_id", F.date_format(slvr_act_df.planned_start_datetime_local, "yyyyMMdd")) \
            .withColumn("planned_activity_start_date_utc_id", F.date_format(slvr_act_df.planned_start_datetime_utc, "yyyyMMdd")) \
            .withColumn("planned_activity_end_date_id", F.date_format(slvr_act_df.planned_end_datetime_local, "yyyyMMdd")) \
            .withColumn("planned_activity_end_date_utc_id", F.date_format(slvr_act_df.planned_end_datetime_utc, "yyyyMMdd")) 
        
        slvr_act_df = self._add_time_id(slvr_act_df, "activity_start_time_id", "start_datetime_local")
        slvr_act_df = self._add_time_id(slvr_act_df, "activity_end_time_id", "end_datetime_local")

        source_df = slvr_act_df \
            .join(activitytypedim_df, slvr_act_df.type == activitytypedim_df.activity_type) \
            .select(
                slvr_act_df.reference.alias("activity_reference"),
                slvr_act_df.resource_reference.alias("worker_reference"),
                slvr_act_df.parent_activity_reference.alias("shift_reference"),
                slvr_act_df.task_reference,
                slvr_act_df.start_datetime_local.alias("activity_start_datetime"),
                slvr_act_df.start_datetime_utc.alias("activity_start_datetime_utc"),
                slvr_act_df.activity_start_date_id,
                slvr_act_df.activity_start_time_id,
                slvr_act_df.activity_start_date_utc_id,
                slvr_act_df.end_datetime_local.alias("activity_end_datetime"),
                slvr_act_df.end_datetime_utc.alias("activity_end_datetime_utc"),
                slvr_act_df.activity_end_date_id,
                slvr_act_df.activity_end_time_id,
                slvr_act_df.activity_end_date_utc_id,
                slvr_act_df.start_latitude.alias("activity_start_latitude"),
                slvr_act_df.start_longitude.alias("activity_start_longitude"),
                slvr_act_df.start_location_validity.alias("activity_start_location_validity"),
                slvr_act_df.finish_latitude.alias("activity_end_latitude"),
                slvr_act_df.finish_longitude.alias("activity_end_longitude"),
                slvr_act_df.finish_location_validity.alias("activity_end_location_validity"),
                slvr_act_df.planned_start_datetime_local.alias("planned_activity_start_datetime"),
                slvr_act_df.planned_start_datetime_utc.alias("planned_activity_start_datetime_utc"),
                slvr_act_df.planned_activity_start_date_id,
                slvr_act_df.planned_activity_start_date_utc_id,
                slvr_act_df.planned_end_datetime_local.alias("planned_activity_end_datetime"),
                slvr_act_df.planned_end_datetime_utc.alias("planned_activity_end_datetime_utc"),
                slvr_act_df.planned_activity_end_date_id,
                slvr_act_df.planned_activity_end_date_utc_id,
                slvr_act_df.planned_start_latitude.alias("planned_activity_start_latitude"),
                slvr_act_df.planned_start_longitude.alias("planned_activity_start_longitude"),
                slvr_act_df.planned_start_location_validity.alias("planned_activity_start_location_validity"),
                slvr_act_df.planned_finish_latitude.alias("planned_activity_end_latitude"),
                slvr_act_df.planned_finish_longitude.alias("planned_activity_end_longitude"),
                slvr_act_df.planned_finish_location_validity.alias("planned_activity_end_location_validity"),
                activitytypedim_df.activity_type_id
        )

        streaming_query = self._repository.write_stream_activityfact(source_df, tenant_name, product_name)

        logger.info("Finished setting up gold 'activityfact' table stream")

        streaming_query.awaitTermination()

    def load_gold_activitytimelinefact_table(self, tenant_name: str, product_name: str) -> None:
        logger.info("Initialising gold 'activitytimelinefact' table stream")

        activityfact_df = self._repository.read_stream("activityfact", "gold", tenant_name, product_name)
        activityfact_df = self._cdf_filter(activityfact_df)

        activityfact_df = activityfact_df \
            .filter((F.col("version_end") == '9999-12-31 23:59:59') & F.col("activity_type_id").isin(1, 3, 4))

        self._repository.write_stream_activitytimelinefact(activityfact_df, tenant_name, product_name)

        logger.info("Finished setting up gold 'activitytimelinefact' table stream")

    def load_gold_taskfact_table(self, tenant_name: str, product_name: str) -> None:
        logger.info("Initialising gold 'taskfact' table stream")

        slvr_task_df = self._repository.read_stream("task", "silver", tenant_name, product_name)
        slvr_task_df = self._cdf_filter(slvr_task_df)
        slvr_task_df = slvr_task_df.withColumnRenamed("reference", "task_reference")

        slvr_act_df = self._repository.get_silveractivitytable(tenant_name, product_name)

        slvr_act_df = slvr_act_df.groupBy(slvr_act_df.task_reference) \
            .agg(
                F.first(slvr_act_df.parent_activity_reference).alias("shift_reference"),
                F.first(slvr_act_df.start_datetime_local).alias("task_start_datetime"),
                F.first(slvr_act_df.start_datetime_utc).alias("task_start_datetime_utc"),
                F.first(slvr_act_df.end_datetime_local).alias("task_end_datetime"),
                F.first(slvr_act_df.end_datetime_utc).alias("task_end_datetime_utc"),
                F.first(slvr_act_df.planned_end_datetime_local).alias("planned_task_end_datetime"),
                F.first(slvr_act_df.planned_end_datetime_utc).alias("planned_task_end_datetime_utc")
            )

        joined_df = slvr_task_df \
            .join(slvr_act_df, "task_reference", "left") 

        source_df = joined_df.select(
            slvr_task_df.task_reference,
            slvr_act_df.shift_reference,
            slvr_task_df.resource_reference.alias("worker_reference"),
            slvr_task_df.created_datetime_local.alias("task_created_datetime"),
            slvr_task_df.created_datetime_utc.alias("task_created_datetime_utc"),
            slvr_act_df.task_start_datetime,
            slvr_act_df.task_start_datetime_utc,
            slvr_act_df.task_end_datetime,
            slvr_act_df.task_end_datetime_utc,
            slvr_task_df.planned_start_datetime_local.alias("planned_task_start_datetime"),
            slvr_task_df.planned_start_datetime_utc.alias("planned_task_start_datetime_utc"),
            slvr_act_df.planned_task_end_datetime,
            slvr_act_df.planned_task_end_datetime_utc
        )

        streaming_query = self._repository.write_stream_taskfact(source_df, tenant_name, product_name)

        logger.info("Finished setting up gold 'taskfact' table stream")

        streaming_query.awaitTermination()

    def load_gold_taskstatusfact_table_milestones(self, tenant_name: str, product_name: str) -> None:
        logger.info("Initialising gold 'taskstatusfact_milestones' table stream")

        slvr_miles_df = self._repository.read_stream("milestone", "silver", tenant_name, product_name)
        slvr_miles_df = self._cdf_filter(slvr_miles_df)

        slvr_activity_df = self._repository.get_silveractivitytable(tenant_name, product_name)
        taskstatusdim_df = self._repository.get_taskstatusdimension(tenant_name, product_name)

        milestone_status_df = slvr_miles_df \
            .join(taskstatusdim_df, F.upper(slvr_miles_df.type) == taskstatusdim_df.detailed_status, "inner") \
            .join(slvr_activity_df, slvr_activity_df.task_reference == slvr_miles_df.task_reference, "left" )

        milestone_status_df = milestone_status_df.select(
            slvr_miles_df.task_reference,
            milestone_status_df.parent_activity_reference.alias("shift_reference"),
            milestone_status_df.status_id,
            milestone_status_df.created_datetime_local.alias("status_start_datetime"),
            milestone_status_df.created_datetime_utc.alias("status_start_datetime_utc"),
            milestone_status_df.created_datetime_local.alias("status_end_datetime"),
            milestone_status_df.created_datetime_utc.alias("status_end_datetime_utc")
        )
            
        streaming_query = self._repository.write_stream_taskstatusfact_milestones(milestone_status_df, tenant_name, product_name)

        logger.info("Finished setting up gold 'taskstatusfact_milestones' table stream")

        streaming_query.awaitTermination()


    def load_gold_taskstatusfact_table_activities(self, tenant_name: str, product_name: str) -> None:
        logger.info("Initialising gold 'taskstatusfact_activities' table stream")

        slvr_activity_df = self._repository.read_stream("activity", "silver", tenant_name, product_name)
        slvr_activity_df = self._cdf_filter(slvr_activity_df)

        slvr_task_df = self._repository.get_silvertasktable(tenant_name, product_name)
        taskstatusdim_df = self._repository.get_taskstatusdimension(tenant_name, product_name)

        activity_status_df = slvr_activity_df \
            .join(slvr_task_df, slvr_activity_df.task_reference == slvr_task_df.reference, "inner" ) 
        
        activity_status_df = activity_status_df.withColumn(
            "detailed_status",
            F.when(F.col("type") == "TravelTask", "START_TRAVEL")
            .when(F.col("outcome_code") == "Fixed", 
                F.when(F.col("visit_code") == "FurtherVisit", "FIXED_NOT_FIRST_TIME")
                .otherwise("FIXED"))
            .when(F.col("outcome_code") == "FurtherVisitRequired", "FURTHER_VISIT_REQUIRED")
            .when((F.col("action_type") == "Close") & (F.col("outcome_code").isNull()), "CLOSED_PENDING_OUTCOME")
            # ??!! I guess it's worth double checking the closed logic
            .when((F.col("action_type") == "Close") & (F.col("outcome_code").isNotNull()), "CLOSED") 
            .when(F.col("outcome_type") == "Abort", "ABORT")
            .when(F.col("outcome_type") == "Attended", "ATTENDED")
            .when(F.col("action_type") == "Cancel", "CANCEL")
            .when(F.col("outcome_type") == "NoAccess", "NO_ACCESS")
            )
        
        activity_status_df = activity_status_df \
            .join(taskstatusdim_df, activity_status_df.detailed_status == taskstatusdim_df.detailed_status, "inner") \
            .select(
                activity_status_df.task_reference,
                activity_status_df.parent_activity_reference.alias("shift_reference"),
                taskstatusdim_df.status_id,
                activity_status_df.start_datetime_local.alias("status_start_datetime"),
                activity_status_df.start_datetime_utc.alias("status_start_datetime_utc"),
                activity_status_df.end_datetime_local.alias("status_end_datetime"),
                activity_status_df.end_datetime_utc.alias("status_end_datetime_utc")
            )
            
        streaming_query = self._repository.write_stream_taskstatusfact_activities(activity_status_df, tenant_name, product_name)

        logger.info("Finished setting up gold 'taskstatusfact_activities' table stream")

        streaming_query.awaitTermination()

    def load_gold_tasklateststatusfact_table(self, tenant_name: str, product_name: str) -> None:
        logger.info("Initialising gold 'tasklateststatusfact' table stream")

        gold_taskstatus_df = self._repository.read_stream("taskstatusfact", "gold", tenant_name, product_name)
        gold_taskstatus_df = self._cdf_filter(gold_taskstatus_df)
        gold_taskstatus_df = gold_taskstatus_df.filter(gold_taskstatus_df.version_end == "9999-12-31 23:59:59")
            
        self._repository.write_stream_tasklateststatusfact(gold_taskstatus_df, tenant_name, product_name)

    @timed_execution("shiftkpifact")
    def load_gold_shiftkpifact_table(self, tenant_name: str, product_name: str) -> None:
        batch_dt = datetime.utcnow()

        logger.info("Getting latest changed shift IDs")
        changed_shifts_ids_df = self._repository.get_changed_shifts_ids(tenant_name, product_name)

        logger.info("Getting shiftfact batch")
        shiftfact_batch_df = self._repository.get_shiftfact_batch(changed_shifts_ids_df, tenant_name, product_name)

        logger.info("Getting activityfact batch")
        activityfact_batch_df = self._repository.get_activityfact_batch(changed_shifts_ids_df, tenant_name, product_name)

        logger.info("Getting taskfact batch")
        taskfact_batch_df = self._repository.get_taskfact_batch(changed_shifts_ids_df, tenant_name, product_name)

        logger.info("Getting taskstatusfact batch")
        taskstatusfact_batch_df = self._repository.get_taskstatusfact_batch(changed_shifts_ids_df, tenant_name, product_name)

        logger.info("Getting tenantconfig")
        tenantconfig_df = self._repository.get_latest_tenantconfig(tenant_name, product_name)

        logger.info("Getting kpitargetconfig")
        kpitargetconfig_df = self._repository.get_latest_kpitargetconfig(tenant_name, product_name)

        calc = _ShiftKPICalculator(shiftfact_batch_df, activityfact_batch_df, taskfact_batch_df, taskstatusfact_batch_df, tenantconfig_df, kpitargetconfig_df)
        kpi_manager = domain.EPIKPIManager()
        kpi_results = []

        for kpi_code, kpi_def in kpi_manager.kpis().items():
            logger.info(f"Calculating KPI \"{kpi_code}\"")
            kpi_df = calc.calculate(kpi_code, kpi_def)
            kpi_results.append(kpi_df)

        logger.info("Completed KPI calculations")

        res_df = pd.concat(kpi_results, ignore_index=True)

        if res_df.empty:
            logger.info("No new KPI results")
            return

        logger.info("Writing KPI results to shiftkpifact")
        self._repository.write_shiftkpifact(res_df, batch_dt, tenant_name, product_name)

    @timed_execution("taskgeoroute")
    def load_gold_taskgeoroutefact_table(self, tenant_name: str, product_name: str) -> None:
        activity_gold_df = self._repository.read_stream("activityfact", "gold", tenant_name, product_name)
        
        activity_gold_df = activity_gold_df.filter(activity_gold_df.version_end == "9999-12-31 23:59:59")

        self._repository.write_stream_taskgeoroutefact(activity_gold_df, tenant_name, product_name)

    @timed_execution("shiftkpafact")
    def load_gold_shiftkpafact_table(self, tenant_name: str, product_name: str) -> None:
        skf_df = self._repository.read_stream("shiftkpifact", "gold", tenant_name, product_name)

        # kd_df = self._repository.get_kpidimensions(tenant_name, product_name)

        # res_df = skf_df \
        #     .filter(skf_df.version_end == F.lit("9999-12-31 23:59:59")) \
        #     .join(kd_df, kd_df.id == skf_df.kpi_id, "inner") \
        #     .groupBy(skf_df.shift_id, kd_df.kpa_id) \
        #     .agg(
        #         F.first(skf_df.worker_id).alias("worker_id"),
        #         F.first(skf_df.shift_start_date_id).alias("shift_start_date_id"),
        #         F.avg(skf_df.kpi_value).alias("avg_kpi_value"),
        #         F.avg(skf_df.kpi_norm_value).alias("avg_kpi_norm_value")
        #     )

        # res_df = res_df.select(
        #     res_df.shift_id,
        #     res_df.worker_id,
        #     res_df.shift_start_date_id,
        #     res_df.kpa_id,
        #     res_df.avg_kpi_value.cast(FloatType()),
        #     res_df.avg_kpi_norm_value.cast(DecimalType(5,2))
        # )

        self._repository.write_stream_shiftkpafact(skf_df, tenant_name, product_name)


    @timed_execution("shiftkpifactoverall")
    def load_gold_shiftkpioverallfact_table(self, tenant_name: str, product_name: str) -> None:
        skf_df = self._repository.read_stream("shiftkpifact", "gold", tenant_name, product_name)

        # res_df = skf_df \
        #     .filter(skf_df.version_end == F.lit("9999-12-31 23:59:59")) \
        #     .groupBy(skf_df.shift_id) \
        #     .agg(
        #         F.first(skf_df.worker_id).alias("worker_id"),
        #         F.first(skf_df.shift_start_date_id).alias("shift_start_date_id"),
        #         F.avg(skf_df.kpi_value).alias("avg_kpi_value"),
        #         F.avg(skf_df.kpi_norm_value).alias("avg_kpi_norm_value")
        #     )

        # res_df = res_df.select(
        #     res_df.shift_id,
        #     res_df.worker_id,
        #     res_df.shift_start_date_id,
        #     res_df.avg_kpi_value.cast(FloatType()),
        #     res_df.avg_kpi_norm_value.cast(DecimalType(5,2))
        # )

        self._repository.write_stream_shiftkpioverallfact(skf_df, tenant_name, product_name)

    @timed_execution("shiftkpafactoverall")
    def load_gold_shiftkpaoverallfact_table(self, tenant_name: str, product_name: str) -> None:
        skf_df = self._repository.read_stream("shiftkpafact", "gold", tenant_name, product_name)

        self._repository.write_stream_shiftkpaoverallfact(skf_df, tenant_name, product_name)


    @timed_execution("shiftexclusionfact")
    def load_gold_shiftexclusionfact_table(self, tenant_name: str, product_name: str) -> None:
        current_batch_dt = datetime.utcnow()

        silver_shifts_df = self._repository.get_silvershifttable(tenant_name, product_name)
        silver_shifts_df = self._cdf_filter(silver_shifts_df)
        latest_shifts_df = silver_shifts_df \
            .groupBy(silver_shifts_df.shift_reference) \
            .agg(F.max(silver_shifts_df.load_date).alias("max_load_date"))
        
        silver_shifts_df = silver_shifts_df.alias("ss") \
            .join(latest_shifts_df.alias("ls"), 
                  (F.col("ss.shift_reference") == F.col("ls.shift_reference")) & 
                  (F.col("ss.load_date") == F.col("ls.max_load_date")), "inner") \
            .select("ss.*")

        silver_shifts_df = silver_shifts_df.withColumn(
            "duration",
            F.unix_timestamp(F.coalesce(silver_shifts_df.end_datetime_utc, F.current_timestamp())) - F.unix_timestamp(silver_shifts_df.start_datetime_utc)
        )

        exclusion_conditions = (
            (silver_shifts_df.duration < 0) |  # the shift duration is negative
            ((silver_shifts_df.end_datetime_utc.isNotNull()) & silver_shifts_df.duration.between(0, 3600)) |  # The shift is shorter than 60 minutes but not negative
            (silver_shifts_df.duration > 720 * 60)  # The shift lasts over 720 minutes
        )
        excluded_shifts_df = silver_shifts_df.filter(exclusion_conditions)

        excluded_shifts_df = excluded_shifts_df.withColumn(
            "reason_code",
            F.when(silver_shifts_df.duration < 0, "ShiftDurationNegative")
            .when((silver_shifts_df.end_datetime_utc.isNotNull()) & silver_shifts_df.duration.between(0, 3600), "ShiftDurationTooShort")
            .when(silver_shifts_df.duration > 720 * 60, "ShiftDurationTooLong")
        )

        shiftexclusionreason_dim_df = self._repository.get_shiftexclusionreasondimension(tenant_name, product_name)

        excluded_shifts_df = excluded_shifts_df \
                    .join(shiftexclusionreason_dim_df, 'reason_code', "left") \
                    .withColumn("version_start", F.lit(current_batch_dt)) \
                    .withColumn("version_end", F.lit("9999-12-31 23:59:59").cast(TimestampType())) \
                    .select(
                        excluded_shifts_df.shift_reference,
                        shiftexclusionreason_dim_df.id.alias("exclusion_reason_id"),
                        F.col("version_start"),
                        F.col("version_end")
                    )
        
        unexcluded_shifts_df = silver_shifts_df.filter(~exclusion_conditions) \
                    .select(
                        silver_shifts_df.shift_reference,
                        F.lit(current_batch_dt).alias("version_end")
                    )               

        logger.info("Writing results to shiftexclusionfact")

        self._repository.write_shiftexclusionfact(excluded_shifts_df, unexcluded_shifts_df, tenant_name, product_name)


def new_interactor(repository: RepositoryInterface) -> Interactor:
    return Interactor(repository)


class _ShiftKPICalculator:
    @timed_execution("initialise Shift KPI Calculator")
    def __init__(
            self, 
            shiftfact_batch_df: DataFrame, 
            activityfact_batch_df: DataFrame,
            taskfact_batch_df: DataFrame,
            taskstatusfact_batch_df: DataFrame,
            tenantconfig_df: DataFrame,
            kpitarget_df: DataFrame
        ) -> None:
            logger.info(f"Creating DDB instance")
            self._ddb = duckdb.connect(":memory:")
            logger.info(f"Creating SHIFTFACT view")
            self._ddb.register(domain.SHIFTFACT_VIEW_NAME, shiftfact_batch_df.toPandas())
            logger.info(f"Creating ACTIVITYFACT view")
            self._ddb.register(domain.ACTIVITYFACT_VIEW_NAME, activityfact_batch_df.toPandas())
            logger.info(f"Creating TASKFACTBASE view")
            self._ddb.register(domain.TASKFACTBASE_VIEW_NAME, taskfact_batch_df.toPandas())
            logger.info(f"Creating TASKSTATUSFACT view")
            self._ddb.register(domain.TASKSTATUSFACT_VIEW_NAME, taskstatusfact_batch_df.toPandas())
            logger.info(f"Creating TENANTCONFIG view")
            self._ddb.register(domain.TENANTCONFIG_VIEW_NAME, tenantconfig_df.toPandas())
            logger.info(f"Creating KPITARGETCONFIG view")
            self._ddb.register(domain.KPITARGETCONFIG_VIEW_NAME, kpitarget_df.toPandas())

            logger.info(f"Installing SPATIAL DDB extension")
            self._ddb.execute(f"""
                INSTALL SPATIAL; LOAD SPATIAL;   
            """)
    
            logger.info(f"Creating TASKFACT view")
            self._ddb.execute(f"""
                CREATE TABLE {domain.TASKFACT_VIEW_NAME} AS
                SELECT
                    t1.task_id,
                    t1.shift_id,
                    t1.commit_datetime_utc,
                    t1.contact_datetime_utc,
                    t1.start_travel_datetime_utc,
                    t1.arrived_datetime_utc,
                    t1.start_work_datetime_utc,
                    t1.closed_datetime_utc,
                    t1.latest_status,
                    t1.latest_detailed_status,
                    tf.task_created_datetime_utc AS created_datetime_utc,
                    DATE_DIFF('minute', t1.start_travel_datetime_utc, t1.arrived_datetime_utc) AS actual_travel_duration,
                    DATE_DIFF('minute', t1.start_work_datetime_utc, t1.closed_datetime_utc) AS actual_task_duration,
                    tf.planned_task_start_datetime_utc,
                    tf.planned_task_end_datetime_utc,
                    DATE_DIFF('minute', tf.planned_task_start_datetime_utc, tf.planned_task_end_datetime_utc) AS planned_task_duration,
                    DATE_DIFF('minute', af_tt.planned_activity_start_datetime_utc, af_tt.planned_activity_end_datetime_utc) AS planned_travel_duration,
                    ABS(ST_DISTANCE_SPHEROID(ST_POINT(af_tt.activity_end_latitude, af_tt.activity_end_longitude), ST_POINT(af_tt.planned_activity_end_latitude, af_tt.planned_activity_end_longitude))) AS travel_end_dislocation_meters,
                    ABS(ST_DISTANCE_SPHEROID(ST_POINT(af_wt.activity_start_latitude, af_wt.activity_start_longitude), ST_POINT(af_wt.planned_activity_start_latitude, af_wt.planned_activity_start_longitude))) AS work_start_dislocation_meters,
                    ABS(ST_DISTANCE_SPHEROID(ST_POINT(af_wt.activity_end_latitude, af_wt.activity_end_longitude), ST_POINT(af_wt.planned_activity_end_latitude, af_wt.planned_activity_end_longitude))) AS work_end_dislocation_meters,
                    af_wt.activity_end_location_validity AS work_end_dislocation_validity
                FROM (
                    SELECT
                        tsf.task_id,
                        FIRST(tsf.shift_id) AS shift_id,
                        MAX(CASE WHEN tsf.detailed_status = 'COMMIT' THEN status_start_datetime_utc END) AS commit_datetime_utc,
                        MAX(CASE WHEN tsf.detailed_status = 'CONTACTED' THEN status_start_datetime_utc END) AS contact_datetime_utc,
                        MAX(CASE WHEN tsf.detailed_status = 'START_TRAVEL' THEN status_start_datetime_utc END) AS start_travel_datetime_utc,
                        MAX(CASE WHEN tsf.detailed_status = 'ARRIVED' THEN status_start_datetime_utc END) AS arrived_datetime_utc,
                        MAX(CASE WHEN tsf.detailed_status = 'START_WORK' THEN status_start_datetime_utc END) AS start_work_datetime_utc,
                        MAX(CASE WHEN tsf.status = 'Closed' THEN status_start_datetime_utc END) AS closed_datetime_utc,
                        MAX(CASE WHEN tsf.rn = 1 THEN tsf.status END) AS latest_status,
                        MAX(CASE WHEN tsf.rn = 1 THEN tsf.detailed_status END) AS latest_detailed_status
                    FROM (
                        SELECT
                            task_id,
                            shift_id,
                            status,
                            detailed_status,
                            shift_id,
                            status_start_datetime_utc,
                            status_end_datetime_utc,
                            ROW_NUMBER() OVER (PARTITION BY task_id ORDER BY status_start_datetime_utc DESC, detailed_status DESC) AS rn
                        FROM
                            {domain.TASKSTATUSFACT_VIEW_NAME}
                    ) tsf
                    GROUP BY
                        tsf.task_id
                ) t1
                LEFT JOIN {domain.TASKFACTBASE_VIEW_NAME} tf
                    ON tf.task_id = t1.task_id
                LEFT JOIN {domain.ACTIVITYFACT_VIEW_NAME} af_tt
                    ON t1.task_id = af_tt.task_id
                    AND af_tt.activity_type = 'TRAVEL_TASK'
                LEFT JOIN {domain.ACTIVITYFACT_VIEW_NAME} af_wt
                    ON t1.task_id = af_wt.task_id
                    AND af_wt.activity_type = 'WORK'
            """)

            logger.info(f"Creating PRODUCTIVITYFACT view")
            self._ddb.execute(f"""
                CREATE TABLE {domain.PRODUCTIVITYFACT_VIEW_NAME} AS
                SELECT
                    t1.shift_id,
                    DATE_DIFF('minute', sf.shift_start_datetime_utc, COALESCE(sf.shift_end_datetime_utc::TIMESTAMP, CURRENT_TIMESTAMP)::TIMESTAMP) AS shift_duration_minutes,
                    productive_time,
                    productive_travel_time,
                    inf_non_productive_time + shift_duration_minutes - (inf_non_productive_time + productive_time + productive_travel_time) AS non_productive_time
                FROM (
                    SELECT 
                        shift_id,
                        SUM(CASE WHEN productivity_type = 'NON-PRODUCTIVE' THEN DATE_DIFF('minutes', activity_start_datetime_utc, activity_end_datetime_utc) ELSE 0 END) AS inf_non_productive_time,
                        SUM(CASE WHEN productivity_type = 'PRODUCTIVE' THEN DATE_DIFF('minutes', activity_start_datetime_utc, activity_end_datetime_utc) ELSE 0 END) AS productive_time,
                        SUM(CASE WHEN productivity_type = 'PRODUCTIVE-TRAVEL' THEN DATE_DIFF('minutes', activity_start_datetime_utc, activity_end_datetime_utc) ELSE 0 END) AS productive_travel_time
                    FROM {domain.ACTIVITYFACT_VIEW_NAME} af
                    GROUP BY 
                        shift_id
                ) t1
                INNER JOIN {domain.SHIFTFACT_VIEW_NAME} sf
                    ON sf.shift_id = t1.shift_id
            """)

    def calculate(self, kpi_code: str, kpi_query: str) -> pd.DataFrame:
        wrapper = f"""
            SELECT
                kpi_query.shift_id AS shift_id,
                sf.worker_id AS worker_id,
                sf.shift_start_date_id AS shift_start_date_id,
                (SELECT kpi_id FROM {domain.KPITARGETCONFIG_VIEW_NAME} WHERE kpi_code = '{kpi_code}') AS kpi_id,
                kpi_query.kpi_value AS kpi_value,
                kpi_query.kpi_norm_value AS kpi_norm_value
            FROM (
                {kpi_query}
            ) kpi_query
            INNER JOIN {domain.SHIFTFACT_VIEW_NAME} sf
                ON sf.shift_id = kpi_query.shift_id
        """

        return self._ddb.execute(wrapper).df()
