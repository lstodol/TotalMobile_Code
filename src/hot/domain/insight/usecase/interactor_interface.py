from abc import abstractmethod


class InteractorInterface:
    def __init__(self) -> None:
        pass

    @abstractmethod
    def load_silver_shift_table(self, tenant_name: str, product_name: str) -> None:
        pass

    @abstractmethod
    def load_silver_activity_table(self, tenant_name: str, product_name: str) -> None:
        pass

    @abstractmethod
    def load_silver_milestone_table(self, tenant_name: str, product_name: str) -> None:
        pass

    @abstractmethod
    def load_silver_task_table(self, tenant_name: str, product_name: str) -> None:
        pass

    @abstractmethod
    def load_gold_tasklateststatusfact_table(self, tenant_name: str, product_name: str) -> None:
        pass

    @abstractmethod
    def load_silver_task_table_allocations(self, tenant_name: str, product_name: str) -> None:
        pass

    @abstractmethod
    def load_silver_task_table_actions(self, tenant_name: str, product_name: str) -> None:
        pass

    @abstractmethod
    def load_gold_shiftfact_table(self, tenant_name: str, product_name: str) -> None:
        pass

    @abstractmethod
    def load_gold_activityfact_table(self, tenant_name: str, product_name: str) -> None:
        pass

    @abstractmethod
    def load_gold_taskfact_table(self, tenant_name: str, product_name: str) -> None:
        pass

    @abstractmethod
    def load_gold_activitytimelinefact_table(self, tenant_name: str, product_name: str) -> None:
        pass

    @abstractmethod
    def load_gold_taskstatusfact_table_milestones(self, tenant_name: str, product_name: str) -> None:
        pass

    @abstractmethod
    def load_gold_taskstatusfact_table_activities(self, tenant_name: str, product_name: str) -> None:
        pass

    @abstractmethod
    def load_gold_shiftkpifact_table(self, tenant_name: str, product_name: str) -> None:
        pass
    
    @abstractmethod
    def load_gold_shiftkpafact_table(self, tenant_name: str, product_name: str) -> None:
        pass
    
    @abstractmethod
    def load_gold_shiftkpioverallfact_table(self, tenant_name: str, product_name: str) -> None:
        pass

    @abstractmethod
    def load_gold_shiftkpaoverallfact_table(self, tenant_name: str, product_name: str) -> None:
        pass

    @abstractmethod
    def load_gold_shiftexclusionfact_table(self, tenant_name: str, product_name: str) -> None:
        pass

    @abstractmethod
    def load_gold_taskgeoroutefact_table(self, tenant_name: str, product_name: str) -> None:
        pass

