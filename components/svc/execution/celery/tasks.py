from ...model import DataSchema
from celery import Celery
from urllib3.packages.six import BytesIO
from ...storage import get_storage
import pandas as pd
from ...validate import validate
import json

app = Celery("ingest", broker="pyamqp://guest@localhost//", backend="redis://")

@app.task
def ingest(path, schema):
    storage = get_storage("minio")
    data = next(storage.get(path))
    df = pd.read_csv(BytesIO(data), delimiter=";")
    validate(df, DataSchema(**json.loads(schema)))
    
