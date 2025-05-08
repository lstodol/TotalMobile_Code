from abc import abstractmethod

class ProcessorInterface:
    def __init__(self) -> None:
        pass

    @abstractmethod
    def handler(self, tenant_name: str, product_name: str, entity_name: str, primary_keys: str) -> None:
        pass