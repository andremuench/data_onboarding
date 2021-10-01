from celery import Celery
from urllib3.packages.six import BytesIO
from ...storage import get_storage
import pandas as pd
from ...validate import validate

app = Celery("ingest", broker="pyamqp://guest@localhost//", backend="redis://")

@app.task
def ingest(path, schema):
    storage = get_storage("minio")
    df = pd.read_csv(BytesIO(storage.get(path).data))
    validate(df, schema)
    
