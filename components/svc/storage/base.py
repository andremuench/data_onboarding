from abc import abstractmethod
from dataclasses import dataclass
from datetime import datetime

@dataclass
class FileInfo:
    bucket: str
    path: str 
    size: int 
    last_modified: datetime

class AbstractStorageBackend:

    @abstractmethod
    def put(self, path, object):
        pass 

    @abstractmethod
    def get(self, path, chunksize=None):
        pass 

    @abstractmethod
    def file_info(self, path) -> FileInfo:
        pass

