from datetime import datetime
import logging
import time

logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(levelname)s | %(message)s", datefmt="%d/%m/%Y %I:%M:%S", level=logging.INFO)

def timed_execution(message: str = None):
    def decorator(func):
        def wrapper(*args, **kwargs):
            class_name = "func"
            if args and hasattr(args[0], '__class__'):
                class_name = args[0].__class__.__name__
            
            msg = message
            if msg == None:
                msg = f"{func.__name__}"

            msg = f"{class_name}.{msg}"

            logger.info(f"Started execution block \"{msg}\"")

            func_start_time = time.time()
            res = func(*args, **kwargs)
            func_end_time = time.time()

            logger.info(f"Finished execution block \"{msg}\" in {(func_end_time - func_start_time) * 1000:.2f} ms")

            return res
        
        return wrapper
    return decorator


class SampleRow:
    def __init__(self, id: int, status: int, start_datetime: datetime, end_datetime: datetime) -> None:
        self._id = id
        self._status = status
        self._start_datetime = start_datetime
        self._end_datetime = end_datetime


    @property
    def id(self):
        return self._id


    @property
    def status(self):
        return self._status


    @property
    def start_datetime(self):
        return self._start_datetime


    @property
    def end_datetime(self):
        return self._end_datetime
    

    def __str__(self):
        return f"{{'id': '{self._id}', 'status': '{self._status}', 'start_datetime': '{self._start_datetime}', 'end_datetime': '{self._end_datetime}'}}"


class WorkingSample:
    def __init__(self, id: int, status: int, sample_datetime: datetime) -> None:
        self._id = id
        self._status = status
        self._sample_datetime = sample_datetime


    @property
    def id(self):
        return self._id


    @property
    def status(self):
        return self._status
    

    @property
    def sample_datetime(self):
        return self._sample_datetime
    

    def __str__(self):
        return f"{{'id': '{self._id}', 'status': '{self._status}', 'sample_datetime': '{self._sample_datetime}'}}"


MYSQL_TO_INSIGHT_ACTIVITY_TYPE_MAP = {
    1: 0,
    2: 5,
    3: 4,
    4: 1,
    5: 2,
    6: 3,
    11: 6,
    13: 7
}