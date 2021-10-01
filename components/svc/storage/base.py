from abc import abstractmethod

class AbstractStorageBackend:

    @abstractmethod
    def put(self, path, object):
        pass 

    @abstractmethod
    def get(self, path):
        pass 

    