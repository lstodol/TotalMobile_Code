from abc import abstractmethod
from datetime import datetime
from pyspark.sql import DataFrame
from domain.domain import SampleRow, WorkingSample


class RepositoryInterface:

    @abstractmethod
    def read_latest_worker_id(self) -> int:
        pass


    @abstractmethod
    def read_new_worker_dimensions(self, latest_id: int) -> DataFrame:
        pass


    @abstractmethod
    def write_worker_dimension(self, workers_df: DataFrame) -> None:
        pass


    @abstractmethod
    def read_latest_sample_id(self) -> int:
        pass


    @abstractmethod
    def read_latest_sample(self) -> SampleRow:
        pass


    @abstractmethod
    def read_working_sample_details(self, sample_id: int) -> WorkingSample:
        pass


    @abstractmethod
    def write_sample_id(self, sample: SampleRow) -> None:
        pass


    @abstractmethod
    def read_new_shift_facts(self, sample_id: int) -> DataFrame:
        pass


    @abstractmethod
    def write_shift_dimension(self, new_shift_ids_df: DataFrame) -> None:
        pass


    @abstractmethod
    def read_shift_dimension(self) -> DataFrame:
        pass


    @abstractmethod
    def write_new_shift_data(self, new_shifts_df: DataFrame, sample_dt: datetime) -> None:
        pass

    
    @abstractmethod
    def read_new_activity_facts_from_mysql(self, working_sample_id: int) -> DataFrame:
        pass


    @abstractmethod
    def read_new_task_facts(self, sample_id: int) -> DataFrame:
        pass
    

    @abstractmethod
    def write_new_task_facts(self, new_tasks_df: DataFrame, sample_dt: datetime) -> None:
        pass


    @abstractmethod
    def write_new_task_status_facts(self, new_task_stats_df: DataFrame, sample_dt: datetime) -> None:
        pass


    @abstractmethod
    def write_activity_dimension(self, new_references: DataFrame):
        pass


    @abstractmethod
    def write_task_dimension(self, new_references: DataFrame):
        pass
    

    @abstractmethod
    def read_activity_dimension(self) -> DataFrame:
        pass


    @abstractmethod
    def read_task_dimension(self) -> DataFrame:
        pass

    
    @abstractmethod
    def read_latest_loaded_activity_facts(self) -> DataFrame:
        pass


    @abstractmethod
    def write_new_activity_data(self, new_activities_df: DataFrame, sample_dt: datetime) -> None:
        pass