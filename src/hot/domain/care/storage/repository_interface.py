from abc import abstractmethod
from pyspark.sql import DataFrame
from pyspark.sql.streaming.query import StreamingQuery


class RepositoryInterface:
    def __init__(self) -> None:
        pass

    @abstractmethod
    def read_stream(self) -> DataFrame:
        pass

    @abstractmethod
    def write_stream(self, df: DataFrame, bronze_table_name: str, query_name: str, tenant_name: str, product_name: str, primary_keys: str) -> StreamingQuery:
        pass