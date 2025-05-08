import logging
import logging.handlers
import io

from datetime import datetime
import threading
import time
from modules.authorisation import Authorisation

logger = logging.getLogger(__name__)


class AzureBlobHandler(logging.StreamHandler):
    """
    A handler class which writes formatted logging records to azure blob storage files.
    """
    MAX_FLUSH_INVOKES = 3
    
    
    def __init__(self, tenant_name):
        """
        Open the specified file and use it as the stream for logging.
        """
        
        self.tenant_name = tenant_name
        self.log_file_name = self.get_log_filename()
        print(f"Log file path: {self.log_file_name}.")

        container_name = "logs" if tenant_name == "shared" else tenant_name
        container_client = Authorisation.get_container_client(container_name)
        self.blob_client = container_client.get_blob_client(self.log_file_name)

        logging.StreamHandler.__init__(self, io.StringIO())

        self.pointer = 0  
        self.flush_invoke_count = 0
        threading.Thread(target=self._schedule_flush).start()

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self.log_file_name)
    
    def __del__(self):
        # force to flush
        self.flush_invoke_count += self.MAX_FLUSH_INVOKES + 1
        self.flush(increment = True)

    def _schedule_flush(self):
        while True:
            self.flush(increment = True)
            time.sleep(5)
            
    def flush(self, increment = False):
        if self.blob_client and self.stream:
            self.acquire()
            try:
                self.stream.seek(self.pointer)
                data = self.stream.read()
                data_length = len(data)
            
                if data_length > 0:
                    if increment: self.flush_invoke_count += 1
            
                    if self.flush_invoke_count > self.MAX_FLUSH_INVOKES:
                        threading.Thread(target=self.append_blob, args=(data,)).start()
                        self.flush_invoke_count = 0
                        self.pointer += data_length
                        super().flush()
            finally:
                self.release()
            
    def append_blob(self, log_entry):
        self.blob_client.upload_blob(log_entry, blob_type="AppendBlob", length=len(log_entry))
            
    def close(self):
        """
        Closes the stream.
        """
        self.acquire()
        try:
            try:
                if self.stream:
                    try:
                        self.flush_invoke_count += self.MAX_FLUSH_INVOKES + 1
                        self.flush(increment = True)
                    finally:
                        stream = self.stream
                        self.stream = None
                        if hasattr(stream, "close"):
                            stream.close()
            finally:
                logging.StreamHandler.close(self)
        finally:
            self.release()

    def emit(self, record):
        if self.stream is None:
            if not self._closed:
                self.stream = self._open()
        if self.stream:
            logging.StreamHandler.emit(self, record)

    def get_log_filename(self):
        now = datetime.now()
        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")
        timestamp = now.strftime("%Y%m%d---%H%M%S---%f")

        log_folder = f"/logs/{year}/{month}/{day}"
        
        return f"{log_folder}/{timestamp}.log"


class LoggerConfiguration:
    ROOT_LOGGER_INITIALISED = False

    def __init__(self, tenant_name, level=logging.DEBUG):
        self.tenant_name = tenant_name
        self.level = level

    def _get_formatter(self):
        log_file_format = "[%(levelname)s] - %(asctime)s - %(name)s - : %(message)s in %(filename)s function:%(funcName)s ln:%(lineno)d executed by %(threadName)s"
        return logging.Formatter(log_file_format)

    def _get_console_handler(self):
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(self._get_formatter())
        return console_handler

   
    def _get_azure_blob_handler(self):
        azure_handler = AzureBlobHandler(tenant_name=self.tenant_name)
        azure_handler.setFormatter(self._get_formatter())
        return azure_handler

    def _configure_logger(self, custom_logger, console_handler, azure_blob_handler):
        for handler in custom_logger.handlers:
            custom_logger.removeHandler(handler)

        custom_logger.addHandler(console_handler)
        custom_logger.addHandler(azure_blob_handler)
        custom_logger.setLevel(self.level)

    def configure(self, logger):
        if LoggerConfiguration.ROOT_LOGGER_INITIALISED == False:
            console_handler = self._get_console_handler()
            azure_blob_handler = self._get_azure_blob_handler()
            self._configure_logger(logger, console_handler, azure_blob_handler)
            logger.info(
                f"""Main logger configured with params-> tenant_name: {self.tenant_name}, level: {self.level}."""
            )
            
            root_name = __name__.split(".")[0]
            root_logger = logging.getLogger(root_name)
            self._configure_logger(root_logger, console_handler, azure_blob_handler)
            root_logger.info(
                f"""Root logger configured with params-> tenant_name: {self.tenant_name}, level: {self.level}, root_name: {root_name}."""
            )

            hot_name = "hot"
            hot_logger = logging.getLogger(hot_name)
            self._configure_logger(hot_logger, console_handler, azure_blob_handler)
            hot_logger.info(
                f"""Hot logger configured with params-> tenant_name: {self.tenant_name}, level: {self.level}, hot_name: {hot_name}."""
            )
            LoggerConfiguration.ROOT_LOGGER_INITIALISED = True
