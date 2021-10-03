from ...model import DataSchema, InputSpec
from celery import Celery
from urllib3.packages.six import BytesIO
from ...storage import get_storage
import pandas as pd
from ...validate import validate
import json
from ...util.pandas import load_data

app = Celery("ingest", broker="pyamqp://guest@localhost//", backend="redis://")

@app.task
def ingest(path, schema, input_spec_json):
    input_spec: InputSpec = json.loads(**json.loads(input_spec_json))
    storage = get_storage("minio")
    data = next(storage.get(path))
    df = load_data(BytesIO(data), input_spec)
    validate(df, DataSchema(**json.loads(schema)))
    
