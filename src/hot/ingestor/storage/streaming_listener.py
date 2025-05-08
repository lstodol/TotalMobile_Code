import logging
from pyspark.sql.streaming.listener import StreamingQueryListener, QueryStartedEvent, QueryProgressEvent, QueryTerminatedEvent

logger = logging.getLogger(__name__)


class IngestorStreamingQueryListener(StreamingQueryListener):
    def onQueryStarted(self, event: QueryStartedEvent):
        logger.info(f"Ingestor streaming query started: id:{event.id}, name:{event.name}, runId:{event.runId}, timestamp:{event.timestamp}.")

    def onQueryProgress(self, event: QueryProgressEvent):
        logger.debug(f"Ingestor streaming query made progress: {event.progress}.")

    def onQueryTerminated(self, event: QueryTerminatedEvent):
        logger.info(f"Ingestor streaming query terminated:id:{event.id},  runId:{event.runId}, exception:{event.exception}, errorClassOnException:{event.errorClassOnException}.")