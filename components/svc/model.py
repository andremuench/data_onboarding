from enum import Enum
from sqlalchemy import MetaData, Table, Column, String
from sqlalchemy.dialects.postgresql import UUID, JSON
from pydantic import BaseModel, validator
from pydantic.types import UUID4
from typing import List, Literal, Optional, Dict
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

class DataType(Enum):
    STRING = "str"
    INTEGER = "int"
    FLOAT = "float"
    BIT = "bit"
    DATE = "date"
    DATETIME = "datetime"


class Field(BaseModel):
    name: str
    datatype: DataType


class DataSchema(BaseModel):
    version: int
    fields: List[Field]

    class Config:
        orm_mode=True

    @classmethod
    def from_record(cls, record):
        fls = []
        for f in record["fields"]:
            fls.append(Field(**f))
        return cls(
            version=record["version"],
            fields=fls,
        )


class DataSource(BaseModel):
    uuid: Optional[UUID4]
    name: str
    path: str
    dataschema: DataSchema

    class Config:
        orm_mode=True

    @classmethod
    def from_record(cls, record):
        drec = dict(record)
        return cls(
            uuid=drec["uid"],
            name=drec["name"],
            path=drec["path"],
            dataschema=DataSchema.from_record(json.loads(drec["dataschema"]))
        )


class IngestStatus(BaseModel):
    ready: bool
    state: str

class MetaDBConfig(BaseModel):
    db_type: Literal["postgres"]
    engine: str

class Config(BaseModel):
    storage: Dict
    metadb: MetaDBConfig

    @validator("storage")
    def validate_storage(cls, v: Dict):
        if not "storage_type" in v.keys():
            raise ValueError("Type must be in storage definition")
        return v
        