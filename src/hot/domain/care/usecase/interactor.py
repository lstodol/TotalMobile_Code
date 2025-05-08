import logging
from hot.domain.care.usecase.interactor_interface import InteractorInterface
from hot.domain.care.storage.repository_interface import RepositoryInterface
from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    TimestampType,
    StringType,
    DoubleType,
)
from pyspark.sql import SparkSession


logger = logging.getLogger(__name__)


class Interactor(InteractorInterface):
    def __init__(self, repository: RepositoryInterface) -> None:
        super().__init__()
        self._repository: RepositoryInterface = repository
        self.spark = SparkSession.getActiveSession()
        self.active_streaming_query = None

    def _add_load_date(self, input_df):
        output_df = input_df.withColumn("load_date", F.current_timestamp())
        return output_df

    def _get_schema(self, tenant_name: str, table_name: str) -> StructType:

        return self.spark.sql(
            f"select * from {tenant_name}.{table_name} limit 1"
        ).schema

    def load_silver_care_bulletin_table(
        self,
        tenant_name: str,
        domain_name: str,
        table_name: str,
        primary_keys: str,
        data_layer: str,
    ) -> None:

        target_table_name = f"{data_layer}_{domain_name}_{table_name}"
        logger.info(f"Initialising {target_table_name} table stream")

        source_data_layer_1 = "bronze"
        source_table_name_1 = "bronze_totalmobile_bulletin"

        bulletin_stream_df: DataFrame = self._repository.read_stream(
            tenant_name, source_data_layer_1, source_table_name_1
        )
        bulletin_df = bulletin_stream_df.filter("status in(0,1)")

        bulletin_df = bulletin_df.select(
            "unique_key",
            "category",
            "subject",
            "status",
            "event_time",
            F.explode(bulletin_stream_df.recipients).alias("recipient"),
        )

        bulletin_df = (
            bulletin_df.withColumn("user_key", F.col("recipient.user_key"))
            .withColumn("user_name", F.col("recipient.name"))
            .withColumnRenamed("event_time", "message_time")
            .drop("recipient")
        )

        bulletin_df_with_timestamp = self._add_load_date(bulletin_df)

        merge_statement = """target_df.alias("tgt").merge(batch_df.alias("src"), self._get_composite_equal_sql("src", "tgt", pk)).whenMatchedUpdate(condition="src.status != tgt.status", set = {"tgt.status": "src.status", "tgt.message_time": "src.message_time", "tgt.load_date": "src.load_date"}).whenNotMatchedInsertAll().execute()"""

        self.active_streaming_query = self._repository.write_silver_stream(
            df=bulletin_df_with_timestamp,
            target_table_name=target_table_name,
            data_layer=data_layer,
            tenant_name=tenant_name,
        )

        logger.info(f"Completed initialisation of {target_table_name}")

    def load_gold_care_dim_date_table(
        self,
        tenant_name: str,
        domain_name: str,
        table_name: str,
        primary_keys: str,
        data_layer: str,
    ) -> None:

        target_table_name = f"{data_layer}_{domain_name}_{table_name}"
        logger.info(f"Initialising the load for {target_table_name} table")

        dim_date_df = self.spark.sql(
            """
                                select a.calendar_date
                                , year(a.calendar_date) as year
                                , month(a.calendar_date) as month
                                , date_format(a.calendar_date, 'MMMM') as month_name
                                , UPPER(date_format(a.calendar_date, 'MMM')) as month_name_short
                                , date_format(a.calendar_date, 'dd') as day_of_month
                                , dayofweek(a.calendar_date) AS day_of_week
                                , date_format(a.calendar_date, 'EEEE') as day_name
                                , dayofyear(a.calendar_date) as day_of_year
                                , weekofyear(a.calendar_date) as week_of_year
                                , quarter(a.calendar_date) as quarter_of_year
                                , case when a.calendar_date = last_day(a.calendar_date) then 'Y' else 'N' end as last_day_of_month
                                , concat(year(a.calendar_date), '/', weekofyear(a.calendar_date)) as year_week
                                , concat(year(a.calendar_date), '/', month(a.calendar_date)) as year_month
                                from (select explode(sequence(to_date('1900-01-01'), current_date()+1825, interval 1 day)) as calendar_date) a
                                """
        )

        dim_date_df_with_timestamp = self._add_load_date(dim_date_df)

        merge_statement = """target_df.alias("tgt").merge(df.alias("src"), self._get_composite_equal_sql("src", "tgt", primary_keys)).withSchemaEvolution().whenNotMatchedInsert(values = {"tgt.calendar_date": "src.calendar_date", "tgt.year": "src.year", "tgt.month": "src.month", "tgt.month_name": "src.month_name", "tgt.month_name_short": "src.month_name_short", "tgt.day_of_month": "src.day_of_month", "tgt.day_of_week": "src.day_of_week", "tgt.day_name": "src.day_name", "tgt.day_of_year": "src.day_of_year", "tgt.week_of_year": "src.week_of_year", "tgt.quarter_of_year": "src.quarter_of_year", "tgt.last_day_of_month": "src.last_day_of_month", "tgt.year_week": "src.year_week", "tgt.year_month": "src.year_month", "tgt.load_date": "src.load_date"}).execute()"""

        self._repository.write_gold_batch(
            df=dim_date_df_with_timestamp,
            target_table_name=target_table_name,
            data_layer=data_layer,
            tenant_name=tenant_name,
            primary_keys=primary_keys,
            merge_statement=merge_statement,
        )

        logger.info(f"Completed the load for {target_table_name}")

    def load_gold_care_dim_user_table(
        self,
        tenant_name: str,
        domain_name: str,
        table_name: str,
        primary_keys: str,
        data_layer: str,
    ) -> None:

        target_table_name = f"{data_layer}_{domain_name}_{table_name}"
        logger.info(f"Initialising {target_table_name} table stream")

        source_data_layer_1 = "bronze"
        source_table_name_1 = "bronze_totalmobile_dbo_TM_USER"

        user_stream_df: DataFrame = self._repository.read_stream(
            tenant_name, source_data_layer_1, source_table_name_1
        )
        user_df = user_stream_df.select("usr_id", "usr_user_key", "usr_deleted")

        source_data_layer_2 = "bronze"
        source_table_name_2 = "bronze_totalmobile_dbo_TM_USER_DETAIL"

        user_detail_stream_df: DataFrame = self._repository.read_stream(
            tenant_name, source_data_layer_2, source_table_name_2
        )
        user_detail_df = user_detail_stream_df.select(
            "udt_usr_id", "udt_forename", "udt_surname"
        )

        dim_usr_df = (
            user_df.join(
                user_detail_df,
                user_df["usr_id"] == user_detail_df["udt_usr_id"],
                "inner",
            )
            .select(
                user_df["usr_user_key"].alias("user_key"),
                user_df["usr_deleted"].alias("deleted"),
                (
                    F.when(
                        user_df["usr_deleted"] == "N",
                        F.concat(
                            user_detail_df["udt_forename"],
                            user_detail_df["udt_surname"],
                        ),
                    )
                    .when(
                        user_df["usr_deleted"] == "Y",
                        F.concat(
                            user_detail_df["udt_forename"],
                            user_detail_df["udt_surname"],
                            F.lit(" (Deleted)"),
                        ),
                    )
                    .otherwise(
                        F.concat(
                            user_detail_df["udt_forename"],
                            user_detail_df["udt_surname"],
                            F.lit(" (Unknown Status)"),
                        )
                    )
                ).alias("user_name"),
            )
            .dropDuplicates()
        )

        dim_usr_df_with_timestamp = self._add_load_date(dim_usr_df)

        merge_statement = """target_df.alias("tgt").merge(batch_df.alias("src"), self._get_composite_equal_sql("src", "tgt", pk)).whenMatchedUpdate(condition="src.deleted != tgt.deleted or src.user_name != tgt.user_name", set = {"tgt.user_name": "src.user_name", "tgt.deleted": "src.deleted", "tgt.load_date": "src.load_date"}).whenNotMatchedInsert(values = {"tgt.user_key": "src.user_key", "tgt.user_name": "src.user_name", "tgt.deleted": "src.deleted", "tgt.load_date": "src.load_date"}).execute()"""

        self.active_streaming_query = self._repository.write_gold_stream(
            df = dim_usr_df_with_timestamp, 
            target_table_name = target_table_name,
            data_layer = data_layer,
            tenant_name = tenant_name, 
            primary_keys = primary_keys,
            merge_statement = merge_statement     
        )

        logger.info(f"Completed initialisation of {target_table_name}")

    def load_gold_care_dim_subject_table(
        self,
        tenant_name: str,
        domain_name: str,
        table_name: str,
        primary_keys: str,
        data_layer: str,
    ) -> None:

        target_table_name = f"{data_layer}_{domain_name}_{table_name}"
        logger.info(f"Initialising {target_table_name} table stream")

        source_data_layer_1 = "silver"
        source_table_name_1 = "silver_care_bulletin"

        subject_stream_df: DataFrame = self._repository.read_stream(
            tenant_name, source_data_layer_1, source_table_name_1
        )
        subject_df = subject_stream_df.select("subject").dropDuplicates()

        subject_df_with_timestamp = self._add_load_date(subject_df)

        merge_statement = """target_df.alias("tgt").merge(batch_df.alias("src"), self._get_composite_equal_sql("src", "tgt", pk)).whenNotMatchedInsert(values = {"tgt.subject": "src.subject", "tgt.load_date": "src.load_date"}).execute()"""

        self.active_streaming_query = self._repository.write_gold_stream(
            df = subject_df_with_timestamp, 
            target_table_name = target_table_name,
            data_layer = data_layer,
            tenant_name = tenant_name,
            primary_keys = primary_keys,
            merge_statement = merge_statement
        )

        logger.info(f"Completed initialisation of {target_table_name}")

    def load_gold_care_dim_status_table(
        self,
        tenant_name: str,
        domain_name: str,
        table_name: str,
        primary_keys: str,
        data_layer: str,
    ) -> None:

        target_table_name = f"{data_layer}_{domain_name}_{table_name}"
        logger.info(f"Initialising {target_table_name} table stream")

        source_data_layer_1 = "silver"
        source_table_name_1 = "silver_care_bulletin"

        status_stream_df: DataFrame = self._repository.read_stream(
            tenant_name, source_data_layer_1, source_table_name_1
        )
        status_df = status_stream_df.select("status").dropDuplicates()
        status_df = status_df.filter("status IN(0, 1)").withColumn(
            "status_description",
            F.when(status_df["status"] == 0, F.lit("NOT READ"))
            .when(status_df["status"] == 1, F.lit("READ"))
            .otherwise("UNKNOWN STATUS"),
        )

        status_df_with_timestamp = self._add_load_date(status_df)

        merge_statement = """target_df.alias("tgt").merge(batch_df.alias("src"), self._get_composite_equal_sql("src", "tgt", pk)).whenNotMatchedInsert(values = {"tgt.status": "src.status", "tgt.status_description": "src.status_description", "tgt.load_date": "src.load_date"}).execute()"""

        self.active_streaming_query = self._repository.write_gold_stream(
            df = status_df_with_timestamp, 
            target_table_name = target_table_name,
            data_layer = data_layer,
            tenant_name = tenant_name,
            primary_keys = primary_keys,
            merge_statement = merge_statement
        )

        logger.info(f"Completed initialisation of {target_table_name}")

    def load_gold_care_fact_bulletins_table(
        self,
        tenant_name: str,
        domain_name: str,
        table_name: str,
        primary_keys: str,
        data_layer: str,
    ) -> None:

        target_table_name = f"{data_layer}_{domain_name}_{table_name}"
        logger.info(f"Initialising {target_table_name} table stream")

        source_data_layer_1 = "silver"
        source_table_name_1 = "silver_care_bulletin"

        bulletins_stream_df: DataFrame = self._repository.read_stream(
            tenant_name, source_data_layer_1, source_table_name_1
        )

        bulletins = bulletins_stream_df.select(
            "unique_key", "user_key", "message_time", "category", "subject", "status"
        ).dropDuplicates()

        source_data_layer_2 = "gold"
        source_table_name_2 = "gold_care_dim_date"

        dim_date_stream_df: DataFrame = self._repository.read_stream(
            tenant_name, source_data_layer_2, source_table_name_2
        )
        dim_date = dim_date_stream_df.select("id", "calendar_date")

        source_data_layer_3 = "gold"
        source_table_name_3 = "gold_care_dim_user"

        dim_user_stream_df: DataFrame = self._repository.read_stream(
            tenant_name, source_data_layer_3, source_table_name_3
        )
        dim_user = dim_user_stream_df.select("id", "user_key", "user_name")

        source_data_layer_4 = "gold"
        source_table_name_4 = "gold_care_dim_subject"

        dim_subject_stream_df: DataFrame = self._repository.read_stream(
            tenant_name, source_data_layer_4, source_table_name_4
        )
        dim_subject = dim_subject_stream_df.select("id", "subject")

        source_data_layer_5 = "gold"
        source_table_name_5 = "gold_care_dim_status"

        dim_status_stream_df: DataFrame = self._repository.read_stream(
            tenant_name, source_data_layer_5, source_table_name_5
        )
        dim_status = dim_status_stream_df.select("id", "status")

        bulletins_df = self.spark.sql(
            """
                                      select b.unique_key as unique_key
                                      , dd.id as dim_date_id
                                      , du.id as dim_user_id
                                      , ds.id as dim_subject_id
                                      , dst.id as dim_status_id
                                      , b.message_time as message_time
                                      , b.category as category
                                      , current_timestamp() as load_date
                                      from {bulletins} as b
                                      inner join {dim_date} as dd on to_date(b.message_time) = dd.calendar_date
                                      inner join {dim_user} as du on b.user_key = du.user_key
                                      inner join {dim_subject} as ds on b.subject = ds.subject
                                      inner join {dim_status} as dst on b.status = dst.status
                                      """,
            bulletins=bulletins,
            dim_date=dim_date,
            dim_user=dim_user,
            dim_subject=dim_subject,
            dim_status=dim_status,
        )

        bulletins_df_with_timestamp = self._add_load_date(bulletins_df)

        merge_statement = """target_df.alias("tgt").merge(batch_df.alias("src"), self._get_composite_equal_sql("src", "tgt", pk)).whenMatchedUpdate(condition="src.dim_status_id != tgt.dim_status_id", set = {"tgt.dim_status_id": "src.dim_status_id", "tgt.message_time": "src.message_time", "tgt.load_date": "src.load_date"}).whenNotMatchedInsertAll().execute()"""

        self.active_streaming_query = self._repository.write_gold_stream(
            df = bulletins_df_with_timestamp, 
            target_table_name = target_table_name,
            data_layer = data_layer,
            tenant_name = tenant_name,
            primary_keys = primary_keys,
            merge_statement = merge_statement
        )

        logger.info(f"Completed initialisation of {target_table_name}")


def new_interactor(repository: RepositoryInterface) -> Interactor:
    return Interactor(repository=repository)
