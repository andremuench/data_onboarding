from .base import AbstractStorageBackend
from .minio import MinioStorageBackend

class NoSuchStorage(Exception):
    pass

storage_mapping = {
    "minio": MinioStorageBackend
}

def get_storage(storage_type: str) -> AbstractStorageBackend:
    try:
        return storage_mapping[storage_type].from_config()
    except KeyError:
        raise NoSuchStorage
