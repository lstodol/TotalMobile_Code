import logging
from typing import Tuple, Callable
from hot.domain.care.handler.processor_interface import ProcessorInterface
from hot.domain.care.usecase.interactor_interface import InteractorInterface

logger = logging.getLogger(__name__)

class Processor(ProcessorInterface):

    def __init__(self, interactor: InteractorInterface) -> None:
        super().__init__()
        self._interactor: InteractorInterface = interactor

        self._handlers: dict[Tuple[str, str], Callable[[str, str], None]] = {
            ("bulletin", "silver"): self._interactor.load_silver_care_bulletin_table,
            ("dim_date", "gold"): self._interactor.load_gold_care_dim_date_table,
            ("dim_user", "gold"): self._interactor.load_gold_care_dim_user_table,
            ("dim_subject", "gold"): self._interactor.load_gold_care_dim_subject_table,
            ("dim_status", "gold"): self._interactor.load_gold_care_dim_status_table,
            ("fact_bulletins", "gold"): self._interactor.load_gold_care_fact_bulletins_table
        }

    def handler(self, tenant_name: str, domain_name: str, table_name: str, primary_keys: str, data_layer: str) -> None:
        logger.info(f"Handler request received. Tenant: \"{tenant_name}\", Domain: \"{domain_name}\"")

        if tenant_name in (None, ""):
            errMsg = f"Required argument \"tenant_name\" is empty"
            logger.error(errMsg)
            raise Exception(errMsg)
        
        if domain_name in (None, ""):
            errMsg = f"Required argument \"domain_name\" is empty"
            logger.error(errMsg)
            raise Exception(errMsg)
        
        handler_key = (table_name, data_layer)
        key_not_exists = handler_key not in self._handlers
        if key_not_exists:
            errMsg = f"Table \"{table_name}\" in layer \"{data_layer}\" is not an accepted combination"
            logger.error(errMsg)
            raise Exception(errMsg)
        
        
        handler_func = self._handlers[(handler_key)]
        if handler_func == None:
            errMsg = f"Table \"{table_name}\" in layer \"{data_layer}\" is not an accepted combination"
            logger.error(errMsg)
            raise Exception(errMsg)
        
        handler_func(tenant_name, domain_name, table_name, primary_keys, data_layer)


def new_processor(interactor: InteractorInterface) -> Processor:
    return Processor(interactor=interactor)