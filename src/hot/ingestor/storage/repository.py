import logging
import json
from string import Template
from typing import Optional
from delta import DeltaTable
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming.query import StreamingQuery
from hot.ingestor.storage.repository_interface import RepositoryInterface
from hot.ingestor.storage.streaming_listener import IngestorStreamingQueryListener
 

logger = logging.getLogger(__name__)

INGESTOR_SPARK_APP_NAME = "ingestor-hot-path"

class Repository(RepositoryInterface):
    """Repository implements the RepositoryInterface to provide a means to 
    interact with EventHubs and Delta Lake.

    The Repository is intended to be used to open a streams from EventHubs, 
    allow the :class:`Interactor` to make any data transformations and validations, then 
    write the transformed data to a Delta Lake table sink.

    Currently, the constructor takes a the EventHubs connection string as an 
    argument. This connection string will be reused for each stream.
    """

    BRONZE_TABLE = Template("${tenant_name}.bronze_${product_name}_${table_name}")
    BRONZE_TABLE_CHECKPOINT_PATH = Template("dbfs:/mnt/${tenant_name}/eventhub_bronze_checkpoint/${tenant_name}.bronze_${product_name}_${table_name}")


    def __init__(self, connection_string: str, spark_session: Optional[SparkSession]) -> None:
        super().__init__()

        self._connection_string = connection_string
        self._spark = spark_session if spark_session else SparkSession.getActiveSession()
        self._spark.streams.addListener(IngestorStreamingQueryListener())
        self._spark.conf.set("spark.sql.streaming.metricsEnabled", "true")

    def read_stream(self) -> DataFrame:
        """Creates a stream connection to EventHubs using the connection string provided in 
        the Repository constructor. 

        Returns a Spark Streaming DataFrame which 
        can then be used to apply data transformations and validations to the
        streaming data before being written to the sink.

        Returns
        -------
        :class:`DataFrame`

        """
        
        eh_conf = {
            "eventhubs.connectionString": self._connection_string,
        }
        
        offset = 0
        startingEventPosition = {
            "offset": f"{offset}",
            "seqNo": -1,
            "enqueuedTime": None,
            "isInclusive": True
            }
        
        eh_conf['eventhubs.startingPosition'] = json.dumps(startingEventPosition)

        return self._spark.readStream.format("eventhubs").options(**eh_conf).load()
    

    def write_stream(self, df: DataFrame, bronze_table_name: str, query_name: str, tenant_name: str, product_name: str) -> StreamingQuery:
        """Completes a stream connection from EventHubs to a specified Delta Lake table.

        Parameters
        ----------
        df : :class:`DataFrame`
            the streaming DataFrame to which data tranformations and validations have been applied
        bronze_table_name : str
            the bronze Delta Lake table to which the streaming DataFrame's results will be written
        query_name : str
            the name to be assigned to the streaming query for monitoring and logging purposes
        tenant_name : str
            the name of the tenant for whom this stream is running
        product_name : str
            the specific product for which this stream is running

        Returns
        -------
        :class:`StreamingQuery`

        """

        bronze_table = self.BRONZE_TABLE.substitute(tenant_name=tenant_name, product_name=product_name, table_name=bronze_table_name)
        bronze_table_checkpoint_path = self.BRONZE_TABLE_CHECKPOINT_PATH.substitute(tenant_name=tenant_name, product_name=product_name, table_name=bronze_table_name)

        logger.info(f"Start writing stream into table: '{bronze_table}'. Checkpoint location: '{bronze_table_checkpoint_path}'")

        return (
            df.writeStream.format("delta")
            .outputMode("append")
            .queryName(query_name)
            .option("checkpointLocation", bronze_table_checkpoint_path)
            .toTable(bronze_table))

def new_repository(connection_string: str, spark_session: Optional[SparkSession]) -> Repository:
    connection_string = SparkContext.getOrCreate()._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string) ## TODO: find way of mocking out this _jvm... call

    return Repository(connection_string=connection_string, spark_session=spark_session)

    