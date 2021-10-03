from ...model import DataSchema, DataSource
from ..base import AbstractExecutionClient, Status
from .tasks import ingest as s_ingest, app
from celery.result import AsyncResult

class CeleryClient(AbstractExecutionClient):

    def ingest(self, path, data_source: DataSource) -> str:
        res = s_ingest.delay(path, data_source.schema_.json(), data_source.input_spec.json())
        return res.task_id

    def get_status(self, id) -> Status:
        res_proxy = AsyncResult(id=id, app=app)
        return Status(ready=res_proxy.ready(), state=res_proxy.state)
