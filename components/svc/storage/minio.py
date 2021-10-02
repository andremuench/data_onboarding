
from ..model import Config
from typing import Any, Iterator, Literal

from pydantic.main import BaseModel
from .base import AbstractStorageBackend, FileInfo
from minio import Minio
import os

class MinioConfig(BaseModel):
    storage_type: Literal["minio"]
    bucket: str
    access_key: str
    secret_key: str


class MinioStorageBackend(AbstractStorageBackend):

    def __init__(self, access_key, secret_key, bucket):
        self._client = Minio(
            "localhost:9000", access_key=access_key, secret_key=secret_key, secure=False
        )
        self.bucket = bucket   

    @classmethod
    def from_config(cls, cfg: Config):
        storage_cfg = MinioConfig(**cfg.storage)
        return cls(storage_cfg.access_key, storage_cfg.secret_key, storage_cfg.bucket)

    def put(self, path, object, content_type=None):
        self._client.put_object(self.bucket, path, object, length=-1, part_size=10*1024*1024, content_type=content_type)

    def get(self, path, chunksize:int=None) -> Iterator[bytes]:
        fi = self.file_info(path)
        if chunksize and fi.size > chunksize:
            for i in range(int(fi.size / chunksize) + 1 ):
                print(f"Next call {i} offset={i*chunksize}, length={chunksize}")
                resp = self._client.get_object(self.bucket, path, offset=i*chunksize, length=chunksize)
                yield resp.data
                resp.release_conn()
        else:
            resp =  self._client.get_object(self.bucket, path)
            yield resp.data

    def file_info(self, path):
        stat = self._client.stat_object(self.bucket, path)
        return FileInfo(
            bucket=self.bucket,
            path=path,
            size=stat.size,
            last_modified=stat.last_modified
        )

