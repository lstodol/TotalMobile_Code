from abc import abstractmethod


class InteractorInterface:
    def __init__(self) -> None:
        pass

    @abstractmethod
    def load_bronze_eventhub_tables(self, tenant_name: str, product_name: str) -> None:
        pass
    
    @abstractmethod
    def load_bronze_eventhub_entity(self, tenant_name: str, product_name: str, entity_name: str, primary_keys:str) -> None:
        pass
