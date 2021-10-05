from fastapi import UploadFile, File
from .util.config import get_config
from .execution import get_execution_client
from .storage import get_storage
from fastapi import FastAPI, HTTPException
from fastapi.requests import Request
import os
from sqlalchemy import create_engine
from sqlalchemy.exc import StatementError
from .model import IngestStatus, meta, datasource_tbl
from databases import Database
from uuid import uuid4
import posixpath
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import asyncio
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
        schema=source.schema_.json(by_alias=True),
        input_spec=source.input_spec.json(by_alias=True)
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


# Wrapper for Async -> Sync Read
class ReadWrapper:

    def __init__(self, file_obj:UploadFile):
        self.file_obj = file_obj

    def read(self, size):
        cr = self.file_obj.read(size)
        print(f"Reading batch of size {size}")
        return asyncio.new_event_loop().run_until_complete(cr)


@app.post("/data/{ref}", status_code=201)
async def put_data(ref: str, file:UploadFile=File(...)):
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

    request_id = uuid4()
    _id = f"upload_{request_id}_{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"

    path = posixpath.join(ds.path, _id)

    wrapped = ReadWrapper(file)
    pool = ThreadPoolExecutor()
    def put_async(reader):
        storage.put(path, reader, content_type=file.content_type)
    f = pool.submit(put_async, wrapped)
    await asyncio.wrap_future(f)
    #storage.put(path, BytesIO(body), content_type=cnt_type)

    ing_req_id = exc_client.ingest(path, ds)

    # TODO Question here is if the id of the ingestion job should be returned or wrapped in another uuid?
    # At the moment the uuid of the ingestion job is returned directly

    return ing_req_id


@app.get("/ingest-status/{req_id}")
def get_status(req_id: str) -> IngestStatus:
    status = exc_client.get_status(req_id)
    return IngestStatus(**status.__dict__)
