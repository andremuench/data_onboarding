
from abc import abstractmethod


class AbstractExecutionClient:

    @abstractmethod
    def ingest(self, path, schema): 
        pass 