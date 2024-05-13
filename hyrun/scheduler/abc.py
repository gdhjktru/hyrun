from abc import ABC, abstractmethod

class Scheduler(ABC):
    @abstractmethod
    def schedule(self, task):
        pass