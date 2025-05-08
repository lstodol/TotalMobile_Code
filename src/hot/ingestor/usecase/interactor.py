import logging
from typing import Optional
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType, DoubleType, BooleanType
from pyspark.sql.functions import udf
from hot.ingestor.usecase.interactor_interface import InteractorInterface
from hot.ingestor.storage.repository_interface import RepositoryInterface


logger = logging.getLogger(__name__)

class Interactor(InteractorInterface):
    def __init__(self, repository: RepositoryInterface, spark_session: Optional[SparkSession]) -> None:
        super().__init__()
        self._repository: RepositoryInterface = repository
        self._spark = spark_session if spark_session else SparkSession.getActiveSession()
        self.entity_streaming_query = None

    @classmethod
    def _add_load_date(cls, input_df):
        output_df = input_df.drop("load_date").withColumn("load_date", F.current_timestamp())
        return output_df

    def _extract_and_filter_entity(self, df: DataFrame, tenant_name: str, product_name: str, entity_name: str,) -> DataFrame:

        entity_message_schema = self._get_entity_message_schema(tenant_name, product_name, entity_name)

        # filtering by tenant, validation and extraction of the message body
        df = df.filter(f"properties.tenant_id == '{tenant_name}'")
        df = df.withColumn("entity_data", F.from_json(F.col("body").cast(StringType()), entity_message_schema))
        df = df.select("entity_data.*")
        df = self._add_load_date(df)
        return df
    
    def _get_entity_message_schema(self, tenant_name: str, product_name: str, entity_name: str) -> StructType:
        app_layer = "bronze"
        table_name = f"{tenant_name}.{app_layer}_{product_name}_{entity_name}"
        
        return self._spark.sql(f"SELECT * FROM {table_name} LIMIT 1").schema
    
    def load_bronze_eventhub_entity(self, tenant_name: str, product_name: str, entity_name: str) -> None:
        logger.info(f"Initialising \"{entity_name}\" bronze table stream")

        entity_stream_df: DataFrame = self._repository.read_stream()

        entity_messages_df_with_timestamp = self._extract_and_filter_entity(entity_stream_df, tenant_name, product_name, entity_name)

        self.entity_streaming_query = self._repository.write_stream(
            df=entity_messages_df_with_timestamp, 
            bronze_table_name= entity_name, 
            query_name=f"{entity_name}-eventhubs-stream", 
            tenant_name=tenant_name, 
            product_name=product_name,
        )
        
        logger.info(f"Completed initialisation of \"{entity_name}\"")


def new_interactor(repository: RepositoryInterface, spark_session: Optional[SparkSession]) -> Interactor:
    return Interactor(repository=repository, spark_session=spark_session)

