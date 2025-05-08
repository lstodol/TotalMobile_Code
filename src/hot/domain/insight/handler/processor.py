import logging
from typing import Callable, Tuple
from handler.processor_interface import ProcessorInterface
from usecase.interactor_interface import InteractorInterface


logger = logging.getLogger(__name__)


class Processor(ProcessorInterface):
    def __init__(self, interactor: InteractorInterface) -> None:
        super().__init__()
        self._interactor: InteractorInterface = interactor

        self._handlers: dict[Tuple[str, str], Callable[[str, str], None]] = {
            ("shift", "silver"): self._interactor.load_silver_shift_table,
            ("activity", "silver"): self._interactor.load_silver_activity_table,
            ("task", "silver"): self._interactor.load_silver_task_table,
            ("taskallocation", "silver"): self._interactor.load_silver_task_table_allocations,
            ("taskaction", "silver"): self._interactor.load_silver_task_table_actions,
            ("milestone", "silver"): self._interactor.load_silver_milestone_table,
            ("shiftfact", "gold"): self._interactor.load_gold_shiftfact_table,
            ("activityfact", "gold"): self._interactor.load_gold_activityfact_table,
            ("taskfact", "gold"): self._interactor.load_gold_taskfact_table,
            ("taskstatusfactmilestone", "gold"): self._interactor.load_gold_taskstatusfact_table_milestones,
            ("taskstatusfactactivity", "gold"): self._interactor.load_gold_taskstatusfact_table_activities,
            ("shiftkpifact", "gold"): self._interactor.load_gold_shiftkpifact_table,
            ("shiftkpafact", "gold"): self._interactor.load_gold_shiftkpafact_table,
            ("shiftkpioverallfact", "gold"): self._interactor.load_gold_shiftkpioverallfact_table,
            ("shiftkpaoverallfact", "gold"): self._interactor.load_gold_shiftkpaoverallfact_table,
            ("shiftexclusionfact", "gold"): self._interactor.load_gold_shiftexclusionfact_table,
            ("tasklateststatusfact", "gold"): self._interactor.load_gold_tasklateststatusfact_table,
            ("taskgeoroutefact", "gold"): self._interactor.load_gold_taskgeoroutefact_table,
            ("activitytimelinefact", "gold"): self._interactor.load_gold_activitytimelinefact_table
        }

    def handler(self, tenant_name: str, domain_name: str, table_name: str, data_layer: str) -> None:
        logger.info(f"Handler request received. Tenant: \"{tenant_name}\", Domain: \"{domain_name}\", Table: \"{table_name}\", Layer: \"{data_layer}\"")

        if tenant_name in (None, ""):
            errMsg = f"Required argument \"tenant_name\" is empty"
            logger.error(errMsg)
            raise Exception(errMsg)
        
        if domain_name in (None, ""):
            errMsg = f"Required argument \"domain_name\" is empty"
            logger.error(errMsg)
            raise Exception(errMsg)
        
        handler_func = self._handlers[(table_name, data_layer)]

        if handler_func == None:
            errMsg = f"Table \"{table_name}\" in layer \"{data_layer}\" is not an accepted combination"
            logger.error(errMsg)
            raise Exception(errMsg)
        
        handler_func(tenant_name, domain_name)
        

def new_processor(interactor: InteractorInterface) -> Processor:
    return Processor(interactor=interactor)