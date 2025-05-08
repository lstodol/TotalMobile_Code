from abc import abstractmethod


class ProcessorInterface:

    @abstractmethod
    def handler(self):
        pass