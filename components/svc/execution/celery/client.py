from ..base import AbstractExecutionClient
from .tasks import ingest as s_ingest

class CeleryClient(AbstractExecutionClient):

    def ingest(self, path, schema):
        res = s_ingest.delay(path, schema)
        return res.task_id
