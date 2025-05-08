from handler.processor_interface import ProcessorInterface
from usecase.interactor_interface import InteractorInterface

class Processor(ProcessorInterface):
    def __init__(self, interactor: InteractorInterface) -> None:
        super().__init__()
        self._interactor = interactor

    def handler(self):
        self._interactor.etl()


def new_processor(interactor: InteractorInterface):
    return Processor(interactor=interactor)