from abc import ABC, abstractmethod

class QueryPostsInterface(ABC):

    @abstractmethod
    def query_manager(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def query_sequential(self, list_filters) -> None:
        raise NotImplementedError

    @abstractmethod
    def query_parallel(self, list_filters) -> None:
        raise NotImplementedError