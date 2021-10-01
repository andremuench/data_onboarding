from sqlalchemy import MetaData, Table, Column, String
from sqlalchemy.dialects.postgresql import UUID, JSON
from pydantic import BaseModel
from pydantic.types import UUID4
from typing import List, Optional, Dict
import json
import uuid

meta = MetaData()

datasource_tbl = Table(
    "t_data_source",
    meta,
    Column("uid", UUID, primary_key=True, default=uuid.uuid4),
    Column("path", String),
    Column("name", String),
    Column("dataschema", JSON),
)

class Field(BaseModel):
    name: str
    datatype: str


class DataSchema(BaseModel):
    version: int
    fields: List[Field]


class DataSource(BaseModel):
    uuid: Optional[UUID4]
    name: str
    path: str
    dataschema: Dict

    class Config:
        orm_mode=True

    @classmethod
    def from_record(cls, record):
        drec = dict(record)
        return cls(
            uuid=drec["uid"],
            name=drec["name"],
            path=drec["path"],
            dataschema=json.loads(drec["dataschema"])
        )