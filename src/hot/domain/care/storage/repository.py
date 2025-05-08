import logging
from typing import Tuple
from string import Template
from delta import DeltaTable
from pyspark import SparkContext
from pyspark.sql import SparkSession, Window, DataFrame, functions as F
from pyspark.sql.streaming.query import StreamingQuery
from hot.domain.care.storage.repository_interface import RepositoryInterface
from hot.domain.care.storage.streaming_listener import CareStreamingQueryListener

logger = logging.getLogger(__name__)

DOMAIN_SPARK_APP_NAME = "domain-hot-path"


class Repository(RepositoryInterface):
    """Repository implements the RepositoryInterface to provide a means to
    interact with EventHubs and Delta Lake.

    The Repository is intended to be used to open a streams from EventHubs,
    allow the :class:`Interactor` to make any data transformations and validations, then
    write the transformed data to a Delta Lake table sink.

    Currently, the constructor takes a the EventHubs connection string as an
    argument. This connection string will be reused for each stream.
    """

    DELTA_TABLE_PATH_PREFIX = Template("/mnt/${tenant_name}")
    DELTA_TABLE_PATH = Template("/lakehouse/${data_layer}/${tenant_name}.${table_name}")
    DELTA_TABLE_CHECKPOINT_PATH = Template(
        "/${data_layer}_checkpoint/${tenant_name}.${table_name}"
    )

    def __init__(self, spark_sess: SparkSession, delta: DeltaTable) -> None:
        super().__init__()

        self._spark: SparkSession = spark_sess
        self._delta = delta
        self.split_char_for_pk = ","
        self._spark.streams.addListener(CareStreamingQueryListener())
        self._spark.conf.set("spark.sql.streaming.metricsEnabled", "true")

    def _get_composite_equal_sql(self, source_table, target_table, composite_key):
        sql = ""

        for column in composite_key.split(self.split_char_for_pk):
            clean_column = column.strip()
            sql += f" AND {source_table}.{clean_column} = {target_table}.{clean_column}"

        return sql[5:]

    def _build_table_paths(
        self, tenant_name: str, data_layer: str, table_name: str
    ) -> Tuple[str, str]:
        delta_table_path_prefix = self.DELTA_TABLE_PATH_PREFIX.substitute(
            tenant_name=tenant_name
        )
        delta_table_path = delta_table_path_prefix + self.DELTA_TABLE_PATH.substitute(
            tenant_name=tenant_name, data_layer=data_layer, table_name=table_name
        )
        delta_table_checkpoint_path = (
            delta_table_path_prefix
            + self.DELTA_TABLE_CHECKPOINT_PATH.substitute(
                tenant_name=tenant_name, data_layer=data_layer, table_name=table_name
            )
        )
        return delta_table_path, delta_table_checkpoint_path

    def read_stream(
        self, tenant_name: str, data_layer: str, source_table_name: str
    ) -> DataFrame:
        table_path, _ = self._build_table_paths(
            tenant_name=tenant_name, data_layer=data_layer, table_name=source_table_name
        )

        logger.info(f"Opening stream on table '{table_path}'")

        return (
            self._spark.readStream.format("delta")
            .option("ignoreChanges", "true")
            .table(f"{tenant_name}.{source_table_name}")
        )

    def write_silver_stream(
        self, df: DataFrame, target_table_name: str, data_layer: str, tenant_name: str
    ) -> StreamingQuery:
        """Completes a stream connection from bronze table(s) to a silver table.

        Parameters
        ----------
        df : :class:`DataFrame`
            the streaming DataFrame to which data tranformations and validations have been applied
        target_table_name : str
            the silver delta lake table to which the streaming DataFrame's results will be written
        data_layer : str
            the medallion layer where target table will be written
        tenant_name : str
            the name of the tenant for whom this stream is running

        Returns
        -------
        :class:`StreamingQuery`

        """

        table_path, table_checkpoint_path = self._build_table_paths(
            tenant_name=tenant_name, data_layer=data_layer, table_name=target_table_name
        )

        logger.info(f"Starting write steam to '{table_path}'")

        return (
            df.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", table_checkpoint_path)
            .trigger(processingTime="5 seconds")
            .toTable(f"{tenant_name}.{target_table_name}")
        )

    def write_gold_batch(
        self,
        df: DataFrame,
        target_table_name: str,
        data_layer: str,
        tenant_name: str,
        primary_keys: str,
        merge_statement: str,
    ) -> DataFrame:
        """Completes a batch connection from bronze or silver table(s) to a gold table.

        Parameters
        ----------
        df : :class:`DataFrame`
            the streaming DataFrame to which data tranformations and validations have been applied
        target_table_name : str
            the gold delta Lake table to which the streaming DataFrame's results will be written
        data_layer : str
            the medallion layer where target table will be written
        tenant_name : str
            the name of the tenant for whom this stream is running
        primary_keys : str
            the coma separated list with primary keys used during merging

        Returns
        -------
        :class:`DataFrame`

        """

        table_path, _ = self._build_table_paths(
            tenant_name=tenant_name, data_layer=data_layer, table_name=target_table_name
        )

        logger.info(f"Starting table '{target_table_name}' write")
        target_df = self._delta.forPath(self._spark, table_path)

        return exec(merge_statement)

    def write_gold_stream(
        self,
        df: DataFrame,
        target_table_name: str,
        data_layer: str,
        tenant_name: str,
        primary_keys: str,
        merge_statement: str,
    ) -> StreamingQuery:
        """Completes a stream connection from Silver table(s) to a gold table.

        Parameters
        ----------
        df : :class:`DataFrame`
            the streaming DataFrame to which data tranformations and validations have been applied
        target_table_name : str
            the gold delta Lake table to which the streaming DataFrame's results will be written
        data_layer : str
            the medallion layer where target table will be written
        tenant_name : str
            the name of the tenant for whom this stream is running
        primary_keys : str
            the coma separated list with primary keys used during merging

        Returns
        -------
        :class:`StreamingQuery`

        """

        table_path, table_checkpoint_path = self._build_table_paths(
            tenant_name=tenant_name, data_layer=data_layer, table_name=target_table_name
        )

        logger.info(f"Starting write steam to '{table_path}'")

        def process_batch(batch_df: DataFrame, batch_id):
            logger.info(f"Running batch '{batch_id}' for table '{target_table_name}'")

            target_df = self._delta.forPath(self._spark, table_path)
            pk = primary_keys

            if target_table_name == "gold_care_fact_bulletins":
                w = (
                    Window()
                    .partitionBy(
                        "unique_key", "dim_user_id", "dim_subject_id", "category"
                    )
                    .orderBy("message_time")
                )
                batch_df = batch_df.withColumn("row_num", F.row_number().over(w))
                batch_df = batch_df.filter("row_num = 1").drop("row_num")
                exec(merge_statement)
            else:
                exec(merge_statement)

        return (
            df.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", table_checkpoint_path)
            .trigger(processingTime="5 seconds")
            .foreachBatch(process_batch)
            .toTable(f"{tenant_name}.{target_table_name}")
        )


def new_repository() -> Repository:
    builder: SparkSession.Builder = SparkSession.builder
    spark_sess: SparkSession = builder.appName(DOMAIN_SPARK_APP_NAME).getOrCreate()
    spark_sess.sparkContext.setLogLevel("DEBUG")

    return Repository(spark_sess=spark_sess, delta=DeltaTable)
