import logging


logger = logging.getLogger(__name__)


class UnityException(Exception):
    ...


class UnityExecutionException(UnityException):
    """
    The execution exception, which indicates that something went wrong, but the processing didn't stop.
    The final execution exception will be raised to indicate problematic execution of the job.
    """

    ...


class ClusterNotFoundException(UnityException):
    """
    Exception raised when the cluster with a given name cannot be found in the Databricks workspace.

    """

    ...

class ProductConfigurationNotFoundException(UnityException):
    """
    Exception raised when the product configuration cannot be found probably because of the wrong name of the product.

    """

    ...

class DomainConfigurationNotFoundException(UnityException):
    """
    Exception raised when the domain configuration cannot be found probably because of the wrong name of the domain.

    """

    ...

class ExceptionCatcher:
    def __init__(self, context: dict = None):
        self.exceptions_list = []
        self.message = None
        if context == None:
            self.context = {}
        else:
            self.context = context

    def set_context(self, **kwargs) -> None:
        self.context.update(kwargs)

    def catch(self, e: Exception) -> None:
        logger.error(f"Caught exception: {e}")

        self.exceptions_list.append({"exception": e, "context": self.context.copy()})

    def throw_if_any(self) -> None:
        number_of_exceptions = len(self.exceptions_list)
        if number_of_exceptions > 0:
            message = f"{number_of_exceptions} exception(s) were caught during job execution."
            for i, ctx_e in enumerate(self.exceptions_list):
                message += f'\n{i+1}. Context: {ctx_e["context"]}, Exception: {ctx_e["exception"]}'

            logger.error(
                f"ExceptionCatcher is raising UnityExecutionException with message: {message}"
            )
            raise UnityExecutionException(message)
