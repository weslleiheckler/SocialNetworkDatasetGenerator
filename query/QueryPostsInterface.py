from abc import ABC, abstractmethod
import pandas as pd

class QueryPostsInterface(ABC):

    @abstractmethod
    def query(self, filter, topic = None) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def query_par(self, filter, queue, topic = None) -> None:
        raise NotImplementedError