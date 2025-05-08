import logging
import traceback
from datetime import datetime, timedelta
from pyspark.sql import functions as F,  DataFrame
from pyspark.sql.types import IntegerType, DoubleType, TimestampType, BooleanType
from usecase.interactor_interface import InteractorInterface
from storage.repository_interface import RepositoryInterface
from storage.repository import SAMPLE_STATUS_FAILED, SAMPLE_STATUS_RETRY, SAMPLE_STATUS_STARTED, SAMPLE_STATUS_SUCCESS
from domain.domain import timed_execution, SampleRow, WorkingSample, MYSQL_TO_INSIGHT_ACTIVITY_TYPE_MAP


logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(levelname)s | %(message)s", datefmt="%d/%m/%Y %I:%M:%S", level=logging.INFO)

class Interactor(InteractorInterface):
    ETL_TIMEOUT_MINS = 8

    def __init__(self, repository: RepositoryInterface) -> None:
        super().__init__()
        self._repository = repository

        self._shift_activities: DataFrame = None


    def _map_event_type_to_activity_type(self, df: DataFrame):
        def map_vals(val):
            return MYSQL_TO_INSIGHT_ACTIVITY_TYPE_MAP.get(val, None)
        
        map_vals_udf = F.udf(lambda x: map_vals(x), IntegerType())

        return df.withColumn("activity_type_id", map_vals_udf(df.event_type_id))
    
    
    def _add_time_id(self, df: DataFrame, colName: str, srcColName: str):
        def _calc_time_id(dt: datetime):
            if dt is None:
                return None
            return dt.hour * 3600 + dt.minute * 60 + dt.second
        
        calc_time_id_udf = F.udf(_calc_time_id, IntegerType())

        return df.withColumn(colName, calc_time_id_udf(df[srcColName]))


    @timed_execution("ETL - Shifts")
    def _etl_shifts(self, working_sample_id: int, sample_dt: datetime):
        new_shifts_df = self._repository.read_new_shift_facts(working_sample_id)

        new_shift_ids_df = new_shifts_df.select(new_shifts_df.shift_reference).distinct()

        self._repository.write_shift_dimension(new_shift_ids_df)

        self._shift_activities = new_shifts_df

        shiftdim_df = self._repository.read_shift_dimension()

        new_shifts_df = new_shifts_df.join(
            shiftdim_df,
            "shift_reference",
            "inner"
        )

        new_shifts_df = new_shifts_df \
            .withColumn("shift_start_date_id", F.date_format(new_shifts_df.shift_start_datetime, "yyyyMMdd")) \
            .withColumn("shift_start_date_utc_id", F.date_format(new_shifts_df.shift_start_datetime_utc, "yyyyMMdd")) \
            .withColumn("shift_end_date_id", F.date_format(new_shifts_df.shift_end_datetime, "yyyyMMdd")) \
            .withColumn("shift_end_date_utc_id", F.date_format(new_shifts_df.shift_end_datetime_utc, "yyyyMMdd"))
        
        new_shifts_df = new_shifts_df.select(
            shiftdim_df.id.alias("shift_id").cast(IntegerType()),
            new_shifts_df.worker_id.cast(IntegerType()),
            new_shifts_df.shift_start_datetime,
            new_shifts_df.shift_start_datetime_utc,
            new_shifts_df.shift_start_date_id.cast(IntegerType()),
            new_shifts_df.shift_start_date_utc_id.cast(IntegerType()),
            new_shifts_df.shift_end_datetime,
            new_shifts_df.shift_end_datetime_utc,
            new_shifts_df.shift_end_date_id.cast(IntegerType()),
            new_shifts_df.shift_end_date_utc_id.cast(IntegerType()),
            new_shifts_df.paid_minutes.cast(IntegerType())
        )
        
        self._repository.write_new_shift_data(new_shifts_df, sample_dt)


    @timed_execution("ETL - Activities")
    def _etl_activities(self, working_sample: WorkingSample, sample_dt: datetime):
        new_act_df = self._repository.read_new_activity_facts_from_mysql(working_sample.id)

        new_act_df = new_act_df.union(
            self._shift_activities.select(
                self._shift_activities.shift_reference,
                F.format_string("%s_SHIFT", self._shift_activities.shift_reference).alias("reference"),
                self._shift_activities.worker_id,
                F.expr("NULL").alias("task_reference"),
                self._shift_activities.shift_start_datetime.alias("start_datetime"),
                self._shift_activities.shift_start_datetime_utc.alias("start_datetime_utc"),
                self._shift_activities.shift_end_datetime.alias("end_datetime"),
                self._shift_activities.shift_end_datetime_utc.alias("end_datetime_utc"),
                F.lit(1).alias("basis_type_id"),
                F.lit(1).alias("event_type_id"),
                F.expr("NULL").alias("start_latitude"),
                F.expr("NULL").alias("start_longitude"),
                F.expr("NULL").alias("start_gps_validity"),
                F.expr("NULL").alias("end_latitude"),
                F.expr("NULL").alias("end_longitude"),
                F.expr("NULL").alias("end_gps_validity")
            )
        )

        new_act_df = self._map_event_type_to_activity_type(new_act_df)

        dist_act_refs_df = new_act_df.select(new_act_df.reference).distinct()
        dist_task_refs_df = new_act_df.select(new_act_df.task_reference).distinct()

        self._repository.write_activity_dimension(dist_act_refs_df)
        self._repository.write_task_dimension(dist_task_refs_df)

        sftdim_df = self._repository.read_shift_dimension()
        actdim_df = self._repository.read_activity_dimension()
        taskdim_df = self._repository.read_task_dimension()

        new_act_df = (
            new_act_df
                .join(actdim_df, actdim_df.activity_reference == new_act_df.reference, "inner")
                .join(taskdim_df, "task_reference", "left")
        )

        llaf_df = self._repository.read_latest_loaded_activity_facts()
        llaf_df = llaf_df.select(
            llaf_df.activity_id,
            # llaf_df.basis_type_id,
            llaf_df.activity_end_datetime_utc,
            llaf_df.task_id,
            llaf_df.activity_start_datetime_utc,
            llaf_df.activity_type_id
        )

        changed_acts_df = (
            new_act_df
                .join(llaf_df, llaf_df.activity_id == actdim_df.id, "left")
                .filter(
                    llaf_df.activity_id.isNull() |
                    (F.coalesce(llaf_df.activity_end_datetime_utc, F.lit('1999-12-31 23:59:59')) != F.coalesce(new_act_df.end_datetime_utc, F.lit('1999-12-31 23:59:59'))) |
                    (F.coalesce(llaf_df.task_id, F.lit(-9999)) != F.coalesce(taskdim_df.id, F.lit(-9999))) |
                    (F.coalesce(llaf_df.activity_start_datetime_utc, F.lit('1999-12-31 23:59:59')) != F.coalesce(new_act_df.start_datetime_utc, F.lit('1999-12-31 23:59:59'))) |
                    (llaf_df.activity_type_id != new_act_df.activity_type_id)
                )
        )

        changed_acts_df = changed_acts_df.join(sftdim_df, "shift_reference", "inner")

        changed_acts_df = (
            changed_acts_df 
                .withColumn("activity_start_date_id", F.date_format(changed_acts_df.start_datetime, "yyyyMMdd")) \
                .withColumn("activity_start_date_utc_id", F.date_format(changed_acts_df.start_datetime_utc, "yyyyMMdd")) \
                .withColumn("activity_end_date_id", F.date_format(changed_acts_df.end_datetime, "yyyyMMdd")) \
                .withColumn("activity_end_date_utc_id", F.date_format(changed_acts_df.end_datetime_utc, "yyyyMMdd")) 
        )
        
        changed_acts_df = self._add_time_id(changed_acts_df, "activity_start_time_id", "start_datetime")
        changed_acts_df = self._add_time_id(changed_acts_df, "activity_end_time_id", "end_datetime")

        changed_acts_df = changed_acts_df.select(
            actdim_df.id.alias("activity_id").cast(IntegerType()),
            sftdim_df.id.alias("shift_id").cast(IntegerType()),
            changed_acts_df.worker_id.cast(IntegerType()),
            taskdim_df.id.alias("task_id").cast(IntegerType()),
            changed_acts_df.start_datetime.alias("activity_start_datetime"),
            changed_acts_df.start_datetime_utc.alias("activity_start_datetime_utc"),
            changed_acts_df.activity_start_date_id.cast(IntegerType()),
            changed_acts_df.activity_start_time_id.cast(IntegerType()),
            changed_acts_df.activity_start_date_utc_id.cast(IntegerType()),
            changed_acts_df.end_datetime.alias("activity_end_datetime"),
            changed_acts_df.end_datetime_utc.alias("activity_end_datetime_utc"),
            changed_acts_df.activity_end_date_id.cast(IntegerType()),
            changed_acts_df.activity_end_time_id.cast(IntegerType()),
            changed_acts_df.activity_end_date_utc_id.cast(IntegerType()),
            new_act_df.activity_type_id.cast(IntegerType()),
            changed_acts_df.start_latitude.alias("activity_start_latitude").cast(DoubleType()),
            changed_acts_df.start_longitude.alias("activity_start_longitude").cast(DoubleType()),
            changed_acts_df.start_gps_validity.alias("activity_start_location_validity").cast(BooleanType()),
            changed_acts_df.end_latitude.alias("activity_end_latitude").cast(DoubleType()),
            changed_acts_df.end_longitude.alias("activity_end_longitude").cast(DoubleType()),
            changed_acts_df.end_gps_validity.alias("activity_end_location_validity").cast(BooleanType()),
            F.expr("NULL").alias("planned_activity_start_datetime").cast(TimestampType()),
            F.expr("NULL").alias("planned_activity_start_datetime_utc").cast(TimestampType()),
            F.expr("NULL").alias("planned_activity_start_date_id").cast(IntegerType()),
            F.expr("NULL").alias("planned_activity_start_date_utc_id").cast(IntegerType()),
            F.expr("NULL").alias("planned_activity_end_datetime").cast(TimestampType()),
            F.expr("NULL").alias("planned_activity_end_datetime_utc").cast(TimestampType()),
            F.expr("NULL").alias("planned_activity_end_date_id").cast(IntegerType()),
            F.expr("NULL").alias("planned_activity_end_date_utc_id").cast(IntegerType()),
            F.expr("NULL").alias("planned_activity_start_latitude").cast(DoubleType()),
            F.expr("NULL").alias("planned_activity_start_longitude").cast(DoubleType()),
            F.expr("NULL").alias("planned_activity_start_location_validity").cast(BooleanType()),
            F.expr("NULL").alias("planned_activity_end_latitude").cast(DoubleType()),
            F.expr("NULL").alias("planned_activity_end_longitude").cast(DoubleType()),
            F.expr("NULL").alias("planned_activity_end_location_validity").cast(BooleanType())
        )

        self._repository.write_new_activity_data(changed_acts_df, sample_dt)


    @timed_execution("ETL - Tasks")
    def _etl_tasks(self, working_sample: WorkingSample, sample_dt: datetime):
        new_tasks_df = self._repository.read_new_task_facts(working_sample.id)

        dist_task_refs_df = new_tasks_df.select(new_tasks_df.task_reference).distinct()
        dist_shift_refs_df = new_tasks_df.select(new_tasks_df.shift_reference).distinct()

        self._repository.write_task_dimension(dist_task_refs_df)
        self._repository.write_shift_dimension(dist_shift_refs_df)

        taskdim_df = self._repository.read_task_dimension()
        sftdim_df = self._repository.read_shift_dimension()

        new_tasks_df = (
            new_tasks_df
                .join(taskdim_df, "task_reference", "inner")
                .join(sftdim_df, "shift_reference", "inner")
        )

        new_tasks_df = new_tasks_df \
            .withColumn("task_id", taskdim_df.id) \
            .withColumn("shift_id", sftdim_df.id) 

        task_facts_df = new_tasks_df \
            .withColumn("planned_start_time_utc", F.to_utc_timestamp(new_tasks_df.planned_start_time, "Europe/London")) \
            .withColumn("planned_complete_time_utc", F.to_utc_timestamp(new_tasks_df.planned_complete_time, "Europe/London"))

        task_facts_df = task_facts_df.select(
            task_facts_df.task_id.cast(IntegerType()).alias("task_id"),
            task_facts_df.worker_id.cast(IntegerType()),
            task_facts_df.shift_id.cast(IntegerType()).alias("shift_id"),
            task_facts_df.task_creation_datetime.alias("task_created_datetime"),
            task_facts_df.task_creation_datetime_utc.alias("task_created_datetime_utc"),
            task_facts_df.task_started_working_datetime.alias("task_start_datetime"),
            task_facts_df.task_started_working_datetime_utc.alias("task_start_datetime_utc"),
            task_facts_df.task_completed_datetime.alias("task_end_datetime"),
            task_facts_df.task_completed_datetime_utc.alias("task_end_datetime_utc"),
            task_facts_df.planned_start_time.alias("planned_task_start_datetime"),
            task_facts_df.planned_start_time_utc.alias("planned_task_start_datetime_utc"),
            task_facts_df.planned_complete_time.alias("planned_task_end_datetime"),
            task_facts_df.planned_complete_time_utc.alias("planned_task_end_datetime_utc")
        )

        self._repository.write_new_task_facts(task_facts_df, sample_dt)

        def generate_status_fact(df: DataFrame, status_id: int, start_datetime_col: str, start_datetime_col_utc: str, end_datetime_col: str, end_datetime_col_utc: str):
            status_id_col = F.lit(status_id)
            if status_id == None:
                status_id_col = df.task_status
                df = df.filter(~df.task_status.isin(5, 19, 6, 7, 8))

            res_df = df.select(
                df.task_id,
                df.shift_id,
                status_id_col.alias("status_id"),
                df[start_datetime_col].alias("status_start_datetime"),
                df[start_datetime_col_utc].alias("status_start_datetime_utc"),
                df[end_datetime_col].alias("status_end_datetime"),
                df[end_datetime_col_utc].alias("status_end_datetime_utc")
            )

            return res_df

        committed_df = generate_status_fact(
            new_tasks_df, 
            5, 
            "task_committed_datetime", 
            "task_committed_datetime_utc", 
            "task_committed_datetime", 
            "task_committed_datetime_utc"
        )

        contacted_df = generate_status_fact(
            new_tasks_df, 
            19, 
            "task_contacted_datetime", 
            "task_contacted_datetime_utc", 
            "task_contacted_datetime", 
            "task_contacted_datetime_utc"
        )

        start_travel_df = generate_status_fact(
            new_tasks_df, 
            6, 
            "task_started_travel_datetime", 
            "task_started_travel_datetime_utc", 
            "task_arrived_datetime", 
            "task_arrived_datetime_utc"
        )

        arrived_df = generate_status_fact(
            new_tasks_df, 
            7, 
            "task_arrived_datetime", 
            "task_arrived_datetime_utc", 
            "task_arrived_datetime", 
            "task_arrived_datetime_utc"
        )

        started_working_df = generate_status_fact(
            new_tasks_df, 
            8, 
            "task_started_working_datetime", 
            "task_started_working_datetime_utc", 
            "task_completed_datetime", 
            "task_completed_datetime_utc"
        )

        completed_df = generate_status_fact(
            new_tasks_df, 
            None, 
            "task_completed_datetime", 
            "task_completed_datetime_utc", 
            "task_completed_datetime", 
            "task_completed_datetime_utc"
        )

        task_stats_facts_df = committed_df.union(contacted_df).union(start_travel_df).union(arrived_df).union(started_working_df).union(completed_df)

        task_stats_facts_df = task_stats_facts_df.select(
            task_stats_facts_df.task_id.cast(IntegerType()),
            task_stats_facts_df.shift_id.cast(IntegerType()),
            task_stats_facts_df.status_id.cast(IntegerType()),
            task_stats_facts_df.status_start_datetime,
            task_stats_facts_df.status_start_datetime_utc,
            task_stats_facts_df.status_end_datetime,
            task_stats_facts_df.status_end_datetime_utc
        )

        task_stats_facts_df = task_stats_facts_df.filter(task_stats_facts_df.status_start_datetime.isNotNull())

        self._repository.write_new_task_status_facts(task_stats_facts_df, sample_dt)


    @timed_execution("ETL")
    def etl(self) -> None:
        sample_dt = datetime.utcnow()

        latest_worker_id = self._repository.read_latest_worker_id()
        logger.info(f"Latest worker_id: {latest_worker_id}")

        new_workers_df = self._repository.read_new_worker_dimensions(latest_worker_id)

        new_workers_df = new_workers_df.withColumn("load_date", F.current_timestamp())

        self._repository.write_worker_dimension(new_workers_df)

        latest_sample = self._repository.read_latest_sample()

        logger.info(f"Latest loaded sample ID: {latest_sample.id}")

        latest_sample_is_in_progress = latest_sample.status in (SAMPLE_STATUS_STARTED, SAMPLE_STATUS_RETRY)
        latest_sample_is_retry = latest_sample.status == SAMPLE_STATUS_RETRY
        latest_sample_is_timed_out = latest_sample.start_datetime + timedelta(minutes=self.ETL_TIMEOUT_MINS) < datetime.utcnow()

        should_retry = False

        if latest_sample_is_in_progress:
            logger.info(f"Latest sample has not yet been completed...")
            if not latest_sample_is_timed_out:
                logger.info(f"Latest sample not yet timed out. Waiting for run to finish.")
                return
            
            logger.info("Latest sample has timed out...")
            
            if latest_sample_is_retry:
                logger.info(f"Latest sample {latest_sample.id} was in retry state. Skipping this sample.")
                should_retry = False

            else:
                logger.info("Latest sample will be retried.")
                should_retry = False
        
        working_sample_id = latest_sample.id + 1

        if should_retry:
            working_sample = latest_sample.id

        logger.info(f"Working sample to be loaded: {working_sample_id}")

        working_sample = self._repository.read_working_sample_details(working_sample_id)
        if working_sample == None:
            logger.info(f"Sample could not be found in warehouse for working sample ID: {working_sample_id}. Cancelling sample run...")
            return

        logger.info(f"Found sample in warehouse: {working_sample}")
        
        if working_sample.status == SAMPLE_STATUS_STARTED:
            logger.info(f"Sample ID '{working_sample_id}' in warehouse is not yet completed, so can be processed on next run if complete. Cancelling current run...")
            return
        
        new_sample_status = SAMPLE_STATUS_STARTED
        if should_retry:
            new_sample_status = SAMPLE_STATUS_RETRY

        self._repository.write_sample_id(SampleRow(int(working_sample_id), new_sample_status, sample_dt, None))

        sample_status = SAMPLE_STATUS_SUCCESS

        try:
            self._etl_shifts(working_sample_id, sample_dt)
        except Exception as e:
            logger.error(f"Error occurred whilst ETLing shifts: {e}")
            logger.error(traceback.format_exc())
            sample_status = SAMPLE_STATUS_FAILED

        try:
            self._etl_activities(working_sample, sample_dt)
        except Exception as e:
            logger.error(f"Error occurred whilst ETLing activities: {e}")
            logger.error(traceback.format_exc())
            sample_status = SAMPLE_STATUS_FAILED

        try:
            self._etl_tasks(working_sample, sample_dt)
        except Exception as e:
            logger.error(f"Error occurred whilst ETLing tasks: {e}")
            logger.error(traceback.format_exc())
            sample_status = SAMPLE_STATUS_FAILED

        self._repository.write_sample_id(SampleRow(working_sample_id, sample_status, sample_dt, datetime.utcnow()))




def new_interactor(repostory: RepositoryInterface):
    return Interactor(repository=repostory)