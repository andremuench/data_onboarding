
from typing import Any
from .base import AbstractStorageBackend
from minio import Minio
import os


class MinioStorageBackend(AbstractStorageBackend):

    def __init__(self, access_key, secret_key, bucket):
        self._client = Minio(
            "localhost:9000", access_key=access_key, secret_key=secret_key, secure=False
        )
        self.bucket = bucket   

    @classmethod
    def from_config(cls):
        access_key = os.environ.get("MINIO_ACCESS_KEY")
        secret_key = os.environ.get("MINIO_SECRET_KEY")
        bucket = "dummy"
        return cls(access_key, secret_key, bucket)

    def put(self, path, object, content_type=None):
        self._client.put_object(self.bucket, path, object, length=-1, part_size=10*1024*1024, content_type=content_type)

    def get(self, path) -> Any:
        return self._client.get_object(self.bucket, path)