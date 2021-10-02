from .util import get_config
from .execution import get_execution_client
from .storage import get_storage
from fastapi import FastAPI, HTTPException
from fastapi.params import Depends
from fastapi.requests import Request
import os
from sqlalchemy import create_engine
from sqlalchemy.exc import StatementError
from .model import IngestStatus, meta, datasource_tbl
from databases import Database
from uuid import uuid4
import posixpath
from datetime import datetime
from .model import DataSource

from urllib3.packages.six import BytesIO

access_key = os.environ.get("MINIO_ACCESS_KEY")
secret_key = os.environ.get("MINIO_SECRET_KEY")

cfg = get_config()
storage = get_storage(cfg)
exc_client = get_execution_client("celery")

DATABASE_URL = cfg.metadb.engine
engine = create_engine(DATABASE_URL)
database = Database(DATABASE_URL.replace("+psycopg2", ""))

app = FastAPI()


@app.on_event("startup")
def recreate_db():
    meta.create_all(bind=engine)


@app.on_event("startup")
async def startup():
    meta.create_all(bind=engine)
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()



async def get_body(req: Request):
    return await req.body()


@app.post("/meta", status_code=201)
async def add_data_source(source: DataSource):
    uid = uuid4()
    q = datasource_tbl.insert().values(
        name=source.name,
        path=source.path,
        uid=uid,
        dataschema=source.dataschema.json(),
    )
    await database.execute(q)
    return uid

@app.get("/meta/{uid}")
async def get_data_source(uid: str) -> DataSource:
    q = datasource_tbl.select().where(datasource_tbl.c.uid == uid)
    res = await database.fetch_one(q)
    if not res:
        raise HTTPException(404)
    ds = DataSource.from_record(res)
    return ds


@app.post("/data/{ref}", status_code=201)
def put_data(ref: str, request: Request, body=Depends(get_body)):
    try:
        result = engine.execute(
            datasource_tbl.select().where(datasource_tbl.c.uid == ref)
        )
        d = result.first()
        if not d:
            raise HTTPException(404)
    except StatementError:
        raise HTTPException(404)
    ds = DataSource.from_record(d)

    cnt_type = request.headers.get("content-type")
    request_id = uuid4()
    _id = f"upload_{request_id}_{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"

    path = posixpath.join(ds.path, _id)

    storage.put(path, BytesIO(body), content_type=cnt_type)

    ing_req_id = exc_client.ingest(path, ds.dataschema)

    # TODO Question here is if the id of the ingestion job should be returned or wrapped in another uuid?
    # At the moment the uuid of the ingestion job is returned directly

    return ing_req_id


@app.get("/ingest-status/{req_id}")
def get_status(req_id: str) -> IngestStatus:
    status = exc_client.get_status(req_id)
    return IngestStatus(**status.__dict__)
