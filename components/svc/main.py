from .execution import get_execution_client
from storage import get_storage
from fastapi import FastAPI, HTTPException
from fastapi.params import Depends
from fastapi.requests import Request
import os
from sqlalchemy import create_engine
from sqlalchemy.exc import StatementError
from model import meta, datasource_tbl
from databases import Database
import json
from uuid import uuid4
import posixpath
from datetime import datetime
from model import DataSource

from urllib3.packages.six import BytesIO

access_key = os.environ.get("MINIO_ACCESS_KEY")
secret_key = os.environ.get("MINIO_SECRET_KEY")

DATABASE_URL = "postgresql+psycopg2://postgres:admin123@localhost/meta"
engine = create_engine(DATABASE_URL)
database = Database(DATABASE_URL.replace("+psycopg2", ""))


storage = get_storage("minio")
exc_client = get_execution_client("celery")

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
        dataschema=json.dumps(source.dataschema),
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

    # TODO has to spawn a runner that executes the ingestion (like gitlab runner)
    # TODO return identifier of ingest job
    return ing_req_id


@app.get("/ingest-status/{req}")
async def get_status(req: str):
    # TODO: get status from the runner / or DB where the runner reports to
    pass
