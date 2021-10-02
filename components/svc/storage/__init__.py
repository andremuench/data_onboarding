from .base import AbstractStorageBackend
from .minio import MinioStorageBackend
from ..model import Config

class NoSuchStorage(Exception):
    pass

storage_mapping = {
    "minio": MinioStorageBackend
}

def get_storage(cfg: Config) -> AbstractStorageBackend:
    try:
        return storage_mapping[cfg.storage["storage_type"]].from_config(cfg)
    except KeyError:
        raise NoSuchStorage
