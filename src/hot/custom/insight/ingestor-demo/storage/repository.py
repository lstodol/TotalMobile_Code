# import mysql.connector
from datetime import datetime
import logging
import json
from urllib.parse import unquote
from pyspark.dbutils import DBUtils
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, TimestampType, StructType, StructField
from pyspark.sql.session import SparkSession
from delta import DeltaTable
from storage.repository_interface import RepositoryInterface
from domain.domain import SampleRow, WorkingSample, timed_execution


logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(levelname)s | %(message)s", datefmt="%d/%m/%Y %I:%M:%S", level=logging.INFO)


SPARK_SESSION_APP_NAME = "productteam-etl"

SAMPLE_STATUS_FAILED = -1
SAMPLE_STATUS_STARTED = 0
SAMPLE_STATUS_SUCCESS = 1
SAMPLE_STATUS_RETRY = 2

DB_CREDENTIALS_SECRET_SCOPE = "analytics"
DB_CREDENTIALS_SECRET_KEY = "productteam-warehouse-connection-properties"


class Repository(RepositoryInterface):
    DELTA_TABLE_NAME_PREFIX = "productteam.gold_insight_"

    def __init__(self, spark_sess: SparkSession, delta: DeltaTable, db_utils: DBUtils) -> None:
        super().__init__()

        # cnx = mysql.connector.connect(
        #     host=self._db_creds["host"],
        #     user=self._db_creds["user"],
        #     password=self._db_creds["password"],
        #     database=self._db_creds["database"]
        # )

        self._spark = spark_sess
        self._delta = delta
        self._db_utils = db_utils

        self._db_creds = self._get_db_credentials()


    def _get_db_credentials(self) -> dict[str, str]:
        # dbutils = DBUtils(self._spark)
        res = unquote(self._db_utils.secrets.get(scope=DB_CREDENTIALS_SECRET_SCOPE, key=DB_CREDENTIALS_SECRET_KEY))
        return json.loads(res)


    def _run_mysql_query(self, query: str) -> DataFrame:
        url = f"jdbc:mysql://{self._db_creds['host']}:3306/productteam-dev-warehouse"

        return self._spark.read \
            .format("jdbc") \
            .option("url", url) \
            .option("driver", "com.mysql.jdbc.Driver") \
            .option("query", query) \
            .option("user", self._db_creds["user"]) \
            .option("password", self._db_creds["password"]) \
            .option("zeroDateTimeBehavior", "CONVERT_TO_NULL") \
            .load()
    

    @timed_execution("read_latest_worker_id")
    def read_latest_worker_id(self) -> int:
        df = self._spark.read.format("delta").table(self.DELTA_TABLE_NAME_PREFIX + "workerdimension")

        return df.select(
            F.coalesce(F.max(df.id), F.lit(-1).cast(IntegerType())).alias("max_id")
        ).collect()[0]["max_id"]


    @timed_execution("read_new_worker_dimensions")
    def read_new_worker_dimensions(self, latest_id: int) -> DataFrame:
        query = f"""
            SELECT
                id,
                name AS worker_reference,
                id AS WorkerID,
                name as Name
            FROM 
                worker_dimension 
            WHERE 
                id > {latest_id}
        """ 

        return self._run_mysql_query(query)
    

    @timed_execution("write_worker_dimension")
    def write_worker_dimension(self, workers_df: DataFrame) -> None:
        workerdim_table = self._delta.forName(self._spark, self.DELTA_TABLE_NAME_PREFIX + "workerdimension")

        workerdim_table.alias("tgt").merge(
            workers_df.alias("src"),
            "tgt.id = src.id"
        ).whenNotMatchedInsertAll() \
        .execute()


    @timed_execution("read_latest_sample_id")
    def read_latest_sample_id(self) -> int:
        df = self._spark.read.format("delta").table(self.DELTA_TABLE_NAME_PREFIX + "sample")

        return df.select(
            F.coalesce(F.max(df.id), F.lit(-1).cast(IntegerType())).alias("max_id")
        ).collect()[0]["max_id"]


    @timed_execution("read_latest_sample")
    def read_latest_sample(self) -> SampleRow:
        df = self._spark.read.format("delta").table(self.DELTA_TABLE_NAME_PREFIX + "sample")

        row = df.orderBy(df.id.desc(), df.start_datetime_utc.desc()).limit(1).collect()[0]

        return SampleRow(row["id"], row["status"], row["start_datetime_utc"], row["end_datetime_utc"])
    

    @timed_execution("read_working_sample_details")
    def read_working_sample_details(self, sample_id: int) -> WorkingSample:
        query = f"""
            SELECT id, status, datetime FROM sample WHERE id = {sample_id}
        """

        res = self._run_mysql_query(query).collect()

        if len(res) == 0:
            return None
        
        row = res[0]

        return WorkingSample(row["id"], row["status"], row["datetime"])
    

    @timed_execution("write_sample_id")
    def write_sample_id(self, sample: SampleRow) -> None:
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("status", IntegerType(), False),
            StructField("start_datetime_utc", TimestampType(), False),
            StructField("end_datetime_utc", TimestampType(), True)
        ])

        data = [[sample.id, sample.status, sample.start_datetime, sample.end_datetime]]

        df = self._spark.createDataFrame(data, schema)

        #gold_insight_sample
        sample_table_name = self.DELTA_TABLE_NAME_PREFIX + "sample"
        sample_table = self._delta.forName(self._spark, sample_table_name)

        sample_table.alias("tgt").merge(
            df.alias("src"),
            "tgt.id = src.id"
        ).whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()


    @timed_execution("read_new_shift_facts")
    def read_new_shift_facts(self, sample_id: int) -> DataFrame:
        prev_sample_id = sample_id - 1
        query = f"""
            SELECT 
                  t1.shift_reference
                , t1.worker_id
                , t1.shift_start_datetime
                , t1.shift_start_datetime_utc
                , t1.shift_end_datetime
                , t1.shift_end_datetime_utc
                , t1.paid_minutes 
            FROM shift_fact_46 t1 
            LEFT JOIN shift_fact_46 t2 ON t2.sample_id = {prev_sample_id} AND t2.worker_id = t1.worker_id AND t2.shift_start_datetime = t1.shift_start_datetime
            WHERE 
                    t1.sample_id = {sample_id} 
                AND (
                            t2.sample_id IS NULL
                        OR  t1.shift_end_datetime != t2.shift_end_datetime
                        OR  t1.paid_minutes != t2.paid_minutes
                    )
        """

        return self._run_mysql_query(query)
    

    @timed_execution("write_shift_dimension")
    def write_shift_dimension(self, new_shift_ids_df: DataFrame) -> None:
        shiftdim_table = self._delta.forName(self._spark, self.DELTA_TABLE_NAME_PREFIX + "shiftdimension")

        new_shift_ids_df = new_shift_ids_df.filter(new_shift_ids_df.shift_reference.isNotNull())

        shiftdim_table.alias("tgt").merge(
            new_shift_ids_df.alias("src"),
            "tgt.shift_reference = src.shift_reference"
        ).whenNotMatchedInsert(
            values={
                "shift_reference": "src.shift_reference",
                "load_date": "CURRENT_TIMESTAMP()"
            }
        ) \
        .execute()


    @timed_execution("read_shift_dimension")
    def read_shift_dimension(self) -> DataFrame:
        return self._spark.read.format("delta").table(self.DELTA_TABLE_NAME_PREFIX + "shiftdimension")

    
    @timed_execution("write_new_shift_data")
    def write_new_shift_data(self, new_shifts_df: DataFrame, sample_dt: datetime) -> None:
        shiftfact_table_name = self.DELTA_TABLE_NAME_PREFIX + "shiftfact"
        shiftfact_table = self._delta.forName(self._spark, shiftfact_table_name)

        new_shifts_df = new_shifts_df \
            .withColumn("version_start", F.lit(sample_dt)) \
            .withColumn("version_end", F.lit("9999-12-31 23:59:59").cast("timestamp"))

        shiftfact_table.alias("tgt").merge(
            new_shifts_df.alias("src"),
            "tgt.shift_id = src.shift_id AND tgt.version_end = \"9999-12-31 23:59:59\""
        ).whenMatchedUpdate(
            set={
                "version_end": F.expr("src.version_start - INTERVAL 1 SECOND")
            }
        ) \
        .execute()

        new_shifts_df.write.format("delta").mode("append").saveAsTable(shiftfact_table_name)


    @timed_execution("read_new_activity_facts")
    def read_new_activity_facts_from_mysql(self, working_sample_id: int) -> DataFrame:
        query = f"""
            SELECT 
                ef.shift_reference
                , ef.reference
                , ef.worker_id
                , ef.task_reference
                , ef.start_datetime
                , ef.start_datetime_utc
                , ef.end_datetime
                , ef.end_datetime_utc
                , ef.basis_type_id
                , ef.event_type_id
                , start_latitude
                , start_longitude
                , start_gps_status_id = 2 AS start_gps_validity
                , end_latitude
                , end_longitude
                , end_gps_status_id = 2 AS end_gps_validity
            FROM 
                event_fact ef
            JOIN (
                SELECT 
                      shift_reference
                    , MAX(_row_created) AS max_row_created 
                FROM 
                    event_fact 
                WHERE 
                        _row_created >= (SELECT datetime FROM sample WHERE id = {working_sample_id-1}) 
                    AND _row_created < (SELECT datetime FROM sample WHERE id = {working_sample_id}) 
                GROUP BY
                    shift_reference
            ) latest
                ON latest.shift_reference = ef.shift_reference AND latest.max_row_created = ef._row_created
            WHERE 
                    ef.event_source_type_id = 1
                AND ef.event_type_id IN (1, 2, 3, 4, 5, 6, 11, 13)
                AND ef.activity_group_id != 1
                AND ef.reference NOT LIKE '%SHIFT_START'
                AND ef.reference NOT LIKE '%SHIFT_END'
        """

        return self._run_mysql_query(query)


    @timed_execution("write_activity_dimension")
    def write_activity_dimension(self, new_references: DataFrame):
        actdim_table_name = self.DELTA_TABLE_NAME_PREFIX + "activitydimension"
        actdim_table = self._delta.forName(self._spark, actdim_table_name)

        actdim_table.alias("tgt").merge(
            new_references.alias("src"),
            "tgt.activity_reference = src.reference"
        ).whenNotMatchedInsert(
            values={
                "activity_reference": "src.reference",
                "load_date": F.current_timestamp()
            }
        ).execute()


    @timed_execution("write_task_dimension")
    def write_task_dimension(self, new_references: DataFrame):
        taskdim_table_name = self.DELTA_TABLE_NAME_PREFIX + "taskdimension"
        taskdim_table = self._delta.forName(self._spark, taskdim_table_name)

        new_references = new_references.filter(new_references.task_reference.isNotNull())

        taskdim_table.alias("tgt").merge(
            new_references.alias("src"),
            "tgt.task_reference = src.task_reference"
        ).whenNotMatchedInsert(
            values={
                "task_reference": "src.task_reference",
                "load_date": F.current_timestamp()
            }
        ).execute()


    @timed_execution("read_activity_dimension")
    def read_activity_dimension(self) -> DataFrame:
        actdim_table_name = self.DELTA_TABLE_NAME_PREFIX + "activitydimension"
        return self._spark.read.format("delta").table(actdim_table_name)
    

    @timed_execution("read_task_dimension")
    def read_task_dimension(self) -> DataFrame:
        taskdim_table_name = self.DELTA_TABLE_NAME_PREFIX + "taskdimension"
        return self._spark.read.format("delta").table(taskdim_table_name)
        

    @timed_execution("read_latest_loaded_activity_facts")
    def read_latest_loaded_activity_facts(self) -> DataFrame:
        table_name = self.DELTA_TABLE_NAME_PREFIX + "activityfact"

        df = self._spark.read.format("delta").table(table_name)
        return df.filter(df.version_end == "9999-12-31 23:59:59")


    @timed_execution("write_new_activity_data")
    def write_new_activity_data(self, new_activities_df: DataFrame, sample_dt: datetime) -> None:
        activityfact_table_name = self.DELTA_TABLE_NAME_PREFIX + "activityfact"
        activityfact_table = self._delta.forName(self._spark, activityfact_table_name)

        new_activities_df = new_activities_df \
            .withColumn("version_start", F.lit(sample_dt)) \
            .withColumn("version_end", F.lit("9999-12-31 23:59:59").cast(TimestampType()))
        
        activityfact_table.alias("tgt").merge(
            new_activities_df.alias("src"),
            "tgt.activity_id = src.activity_id AND tgt.version_end = \"9999-12-31 23:59:59\""
        ).whenMatchedUpdate(
            set={
                "version_end": F.expr("src.version_start - INTERVAL 1 SECOND")
            }
        ).execute()

        new_activities_df.write.format("delta").mode("append").saveAsTable(activityfact_table_name)


    @timed_execution("read_new_task_facts")
    def read_new_task_facts(self, sample_id: int) -> DataFrame:
        prev_sample_id = sample_id - 1
        query = f"""
            SELECT 
                  cur.task_reference
                , cur.worker_id
                , cur.shift_reference
                , cur.task_status
                , cur.task_creation_datetime
                , cur.task_creation_datetime_utc
                , cur.task_committed_datetime
                , cur.task_committed_datetime_utc
                , cur.task_contacted_datetime
                , cur.task_contacted_datetime_utc
                , cur.task_started_travel_datetime
                , cur.task_started_travel_datetime_utc
                , cur.task_arrived_datetime
                , cur.task_arrived_datetime_utc
                , cur.task_started_working_datetime
                , cur.task_started_working_datetime_utc
                , cur.task_completed_datetime
                , cur.task_completed_datetime_utc
                , cur.planned_start_time
                , cur.planned_complete_time
            FROM
                task_fact_46 cur
            LEFT JOIN
                task_fact_46 prv ON prv.sample_id = {prev_sample_id} AND prv.task_id = cur.task_id
            WHERE
                    cur.sample_id = {sample_id}
                AND (
                           prv.task_id IS NULL
                        OR cur.task_creation_datetime != prv.task_creation_datetime
                        OR cur.task_committed_datetime != prv.task_committed_datetime
                        OR cur.task_contacted_datetime != prv.task_contacted_datetime
                        OR cur.task_started_travel_datetime != prv.task_started_travel_datetime
                        OR cur.task_arrived_datetime != prv.task_arrived_datetime
                        OR cur.task_started_working_datetime != prv.task_started_working_datetime
                        OR cur.task_completed_datetime != prv.task_completed_datetime
                        OR cur.planned_start_time != prv.planned_start_time
                        OR cur.planned_complete_time != prv.planned_complete_time
                        OR cur.task_status != prv.task_status
                    )
        """

        return self._run_mysql_query(query)
    
        
    @timed_execution("write_new_task_facts")
    def write_new_task_facts(self, new_tasks_df: DataFrame, sample_dt: datetime) -> None:
        taskfact_table_name = self.DELTA_TABLE_NAME_PREFIX + "taskfact"
        taskfact_table = self._delta.forName(self._spark, taskfact_table_name)

        new_tasks_df = new_tasks_df \
            .withColumn("version_start", F.lit(sample_dt)) \
            .withColumn("version_end", F.lit("9999-12-31 23:59:59").cast("timestamp"))

        taskfact_table.alias("tgt").merge(
            new_tasks_df.alias("src"),
            "tgt.task_id = src.task_id AND tgt.version_end = \"9999-12-31 23:59:59\""
        ).whenMatchedUpdate(
            set={
                "version_end": F.expr("src.version_start - INTERVAL 1 SECOND")
            }
        ).execute()

        new_tasks_df.write.format("delta").mode("append").saveAsTable(taskfact_table_name)


    @timed_execution("write_new_task_status_facts")
    def write_new_task_status_facts(self, new_task_stats_df: DataFrame, sample_dt: datetime) -> None:
        taskstatsfact_table_name = self.DELTA_TABLE_NAME_PREFIX + "taskstatusfact"
        taskstatsfact_table = self._delta.forName(self._spark, taskstatsfact_table_name)

        new_task_stats_df = new_task_stats_df \
            .withColumn("version_start", F.lit(sample_dt)) \
            .withColumn("version_end", F.lit("9999-12-31 23:59:59").cast("timestamp"))

        taskstatsfact_table.alias("tgt").merge(
            new_task_stats_df.alias("src"),
            "tgt.task_id = src.task_id AND tgt.status_id = src.status_id AND tgt.version_end = \"9999-12-31 23:59:59\""
        ).whenMatchedUpdate(
            set={
                "version_end": F.expr("src.version_start - INTERVAL 1 SECOND")
            }
        ).execute()

        new_task_stats_df.write.format("delta").mode("append").saveAsTable(taskstatsfact_table_name)



    @timed_execution("close")
    def close(self) -> None:
        pass
        

def new_repository():
    builder: SparkSession.Builder = SparkSession.builder
    spark_sess: SparkSession = builder.appName(SPARK_SESSION_APP_NAME).getOrCreate()
    db_utils = DBUtils(spark=spark_sess)
    return Repository(spark_sess=spark_sess, delta=DeltaTable, db_utils=db_utils)