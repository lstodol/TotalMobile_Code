from abc import abstractmethod


class InteractorInterface:

    @abstractmethod
    def etl(self):
        pass