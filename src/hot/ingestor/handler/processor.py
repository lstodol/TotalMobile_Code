import logging

from hot.ingestor.handler.processor_interface import ProcessorInterface
from hot.ingestor.usecase.interactor_interface import InteractorInterface

logger = logging.getLogger(__name__)

class Processor(ProcessorInterface):

    def __init__(self, interactor: InteractorInterface) -> None:
        super().__init__()
        self._interactor: InteractorInterface = interactor


    def handler(self, tenant_name: str, product_name: str, entity_name: str) -> None:
        logger.info(f"Handler request received. Tenant: \"{tenant_name}\", Product: \"{product_name}\", Entity: \"{entity_name}\".")

        if tenant_name in (None, ""):
            error_message = f"Required argument \"tenant_name\" is empty."
            logger.error(error_message)
            raise TypeError(error_message)
        
        if product_name in (None, ""):
            error_message = f"Required argument \"product_name\" is empty."
            logger.error(error_message)
            raise TypeError(error_message)
        
        if entity_name in (None, ""):
            error_message = f"Required argument \"entity_name\" is empty."
            logger.error(error_message)
            raise TypeError(error_message)

        self._interactor.load_bronze_eventhub_entity(tenant_name, product_name, entity_name)



def new_processor(interactor: InteractorInterface) -> Processor:
    return Processor(interactor=interactor)