from typing import Callable, Optional
from pyspark.sql import DataFrame

class MockDataStreamReader():

    def __init__(
            self, 
            format_func: callable = None, 
            options_func: callable = None, 
            load_func: callable = None
        ) -> None:
        self._format_func = format_func
        self._options_func = options_func
        self._load_func = load_func

    def format(self, source: str):
        if self._format_func is not None:
            self._format_func(source)
        return self
    
    def options(self, **options: Optional[type[bool] | type[float] | type[int] | type[str]]):
        if self._options_func is not None:
            self._options_func(options)
        return self
    
    def load(self, path: str) -> DataFrame:
        return self._load_func(path)


class MockDataStreamWriter():

    def __init__(
            self,
            format_func: callable = None,
            option_func: callable = None,
            outputMode_func: callable = None,
            queryName_func: callable = None,
            trigger_func: callable = None,
            foreachBatch_func: Callable[[DataFrame, int], None] = None,
            start_func: callable = None,
        ) -> None:
        self._format_func = format_func
        self._option_func = option_func
        self._outputMode_func = outputMode_func
        self._queryName_func = queryName_func
        self._trigger_func = trigger_func
        self._foreachBatch_func = foreachBatch_func
        self._start_func = start_func

    def format(self, source: str):
        if self._format_func is not None:
            self._format_func(source)
        return self
    
    def option(self, key: str, value: Optional[type[bool] | type[float] | type[int] | type[str]]):
        if self._option_func is not None:
            self._option_func(key, value)
        return self
    
    def outputMode(self, outputMode: str):
        if self._outputMode_func is not None:
            self._outputMode_func(outputMode)
        return self
    
    def queryName(self, queryName: str):
        if self._queryName_func is not None:
            self._queryName_func(queryName)
        return self

    def trigger(self, availableNow: bool):
        if self._trigger_func is not None:
            self._trigger_func(availableNow)
        return self
    
    def foreachBatch(self, func: Callable[[DataFrame, int], None]):
        if self._foreachBatch_func is not None:
            self._foreachBatch_func(func)
        return self
    
    def start(self, path: str):
        if self._start_func is not None:
            return self._start_func(path)
        pass


class MockSparkSession():

    def __init__(self, dataStreamReader: MockDataStreamReader = None) -> None:
        self._readStream: MockDataStreamReader = dataStreamReader

    @property
    def readStream(self):
        return self._readStream
    

class MockDataFrame():

    def __init__(self, dataStreamWriter: MockDataStreamWriter) -> None:
        self._writeStream: MockDataStreamWriter = dataStreamWriter

    @property
    def writeStream(self):
        return self._writeStream