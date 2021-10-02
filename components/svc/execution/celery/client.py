from ...model import DataSchema
from ..base import AbstractExecutionClient, Status
from .tasks import ingest as s_ingest, app
from celery.result import AsyncResult

class CeleryClient(AbstractExecutionClient):

    def ingest(self, path, schema: DataSchema) -> str:
        res = s_ingest.delay(path, schema.json())
        return res.task_id

    def get_status(self, id) -> Status:
        res_proxy = AsyncResult(id=id, app=app)
        return Status(ready=res_proxy.ready(), state=res_proxy.state)
