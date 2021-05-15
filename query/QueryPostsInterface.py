from abc import ABC, abstractmethod

class QueryPostsInterface(ABC):

    @abstractmethod
    def query(self, filter) -> None:
        raise NotImplementedError

    @abstractmethod
    def query_par(self, filter, queue) -> None:
        raise NotImplementedError