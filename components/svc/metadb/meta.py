from ..model import Config, datasource_tbl, DataSchema
from sqlalchemy.engine import Engine
import json

class MetaDBError(Exception):
    pass

class NotFoundError(MetaDBError):
    pass 

class MetaDB:

    def __init__(self, engine: Engine):
        self.engine = engine

    def get(self, uid):
        q = datasource_tbl.select().where(datasource_tbl.c.uid == uid)
        proxy = self.engine.execute(q)
        res = proxy.fetchone()
        if res:
            return DataSchema(**json.loads(res["dataschema"]))
        else:
            raise NotFoundError

