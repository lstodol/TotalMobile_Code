from abc import abstractmethod
from pyspark.sql import DataFrame
from pyspark.sql.streaming.query import StreamingQuery
import pandas as pd
from datetime import datetime


class RepositoryInterface:
    def __init__(self) -> None:
        pass

    @abstractmethod
    def read_stream(self, source_table_name: str, data_layer: str, tenant_name: str, product_name: str) -> DataFrame:
        pass

    @abstractmethod
    def write_stream_shift(self, df: DataFrame, tenant_name: str, product_name: str) -> StreamingQuery:
        pass

    @abstractmethod
    def write_stream_activity(self, df: DataFrame, tenant_name: str, product_name: str) -> StreamingQuery:
        pass

    @abstractmethod
    def write_stream_task(self, df: DataFrame, tenant_name: str, product_name: str) -> None:
        pass

    @abstractmethod
    def write_stream_task_allocation(self, df: DataFrame, tenant_name: str, product_name: str) -> None:
        pass

    @abstractmethod
    def write_stream_task_action(self, df: DataFrame, tenant_name: str, product_name: str) -> None:
        pass
    
    @abstractmethod
    def write_stream_milestone(self, df: DataFrame, tenant_name: str, product_name: str) -> None:
        pass

    @abstractmethod
    def write_stream_shiftfact(self, df: DataFrame, tenant_name: str, product_name: str) -> StreamingQuery:
        pass

    @abstractmethod
    def get_changed_shifts_ids(self, tenant_name: str, product_name: str) -> DataFrame:
        pass

    @abstractmethod
    def get_shiftfact_batch(self, changed_shifts_ids_df: DataFrame, tenant_name: str, product_name: str) -> DataFrame:
        pass

    @abstractmethod
    def get_activityfact_batch(self, changed_shifts_ids_df: DataFrame, tenant_name: str, product_name: str) -> DataFrame:
        pass

    @abstractmethod
    def write_stream_activityfact(self, df: DataFrame, tenant_name: str, product_name: str) -> StreamingQuery:
        pass

    @abstractmethod
    def write_stream_taskfact(self, df: DataFrame, tenant_name: str, product_name: str) -> StreamingQuery:
        pass   

    @abstractmethod
    def write_stream_taskstatusfact_milestones(self, df: DataFrame, tenant_name: str, product_name: str) -> StreamingQuery:
        pass   

    @abstractmethod
    def write_stream_taskstatusfact_activities(self, df: DataFrame, tenant_name: str, product_name: str) -> StreamingQuery:
        pass   

    @abstractmethod
    def get_taskfact_batch(self, changed_shifts_ids_df: DataFrame, tenant_name: str, product_name: str) -> DataFrame:
        pass

    @abstractmethod
    def get_taskstatusfact_batch(self, changed_shifts_ids_df: DataFrame, tenant_name: str, product_name: str) -> DataFrame:
        pass

    @abstractmethod
    def get_latest_tenantconfig(self, tenant_name: str, product_name: str) -> DataFrame:
        pass

    @abstractmethod
    def get_latest_kpitargetconfig(self, tenant_name: str, product_name: str) -> DataFrame:
        pass

    @abstractmethod
    def get_kpidimensions(self, tenant_name: str, product_name: str) -> DataFrame:
        pass

    @abstractmethod
    def write_shiftkpifact(self, pddf: pd.DataFrame, batch_dt: datetime, tenant_name: str, product_name: str) -> None:
        pass

    @abstractmethod
    def write_stream_shiftkpafact(self, df: DataFrame, tenant_name: str, product_name: str) -> None:
        pass

    @abstractmethod
    def write_stream_shiftkpioverallfact(self, df: DataFrame, tenant_name: str, product_name: str) -> None:
        pass

    @abstractmethod
    def write_stream_shiftkpaoverallfact(self, df: DataFrame, tenant_name: str, product_name: str) -> None:
        pass

    @abstractmethod
    def write_stream_tasklateststatusfact(self, df: DataFrame, tenant_name: str, product_name: str) -> None:
        pass

    @abstractmethod  
    def write_stream_taskgeoroutefact(self, df: DataFrame, tenant_name: str, product_name: str) -> None:
        pass

    @abstractmethod
    def write_shiftexclusionfact(self, df: DataFrame, tenant_name: str, product_name: str) -> None:
        pass

    @abstractmethod
    def write_stream_activitytimelinefact(self, df: DataFrame, tenant_name: str, product_name: str) -> None:
        pass

    @abstractmethod
    def get_shiftexclusionreasondimension(self, tenant_name: str, product_name: str) -> DataFrame:
        pass

    @abstractmethod
    def get_shiftdimensions(self, tenant_name: str, product_name: str) -> DataFrame:
        pass

    @abstractmethod
    def get_silvershifttable(self, tenant_name: str, product_name: str) -> DataFrame:
        pass

    @abstractmethod
    def get_silveractivitytable(self, tenant_name: str, product_name: str) -> DataFrame:
        pass

    @abstractmethod
    def get_silvertasktable(self, tenant_name: str, product_name: str) -> DataFrame:
        pass

    @abstractmethod
    def get_taskstatusdimension(self, tenant_name: str, product_name: str) -> DataFrame:
        pass

    @abstractmethod
    def get_activitytypedimension(self, tenant_name: str, product_name: str) -> DataFrame:
        pass