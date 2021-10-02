from .base import AbstractStorageBackend
from .minio import MinioStorageBackend
from ..model import Config

class NoSuchStorage(Exception):
    pass

storage_mapping = {
    "minio": MinioStorageBackend
}

def get_storage(storage_type: str, cfg: Config) -> AbstractStorageBackend:
    try:
        return storage_mapping[storage_type].from_config(cfg)
    except KeyError:
        raise NoSuchStorage
