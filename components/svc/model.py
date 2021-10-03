from enum import Enum
from sqlalchemy import MetaData, Table, Column, String
from sqlalchemy.dialects.postgresql import UUID, JSON
from pydantic import BaseModel, validator, Field as PydField
from pydantic.types import UUID4
from typing import List, Literal, Optional, Dict
import json
import uuid

from sqlalchemy.sql.expression import null

meta = MetaData()

datasource_tbl = Table(
    "t_data_source",
    meta,
    Column("uid", UUID, primary_key=True, default=uuid.uuid4),
    Column("path", String, nullable=False),
    Column("name", String, nullable=False),
    Column("input_spec", JSON, nullable=True),
    Column("schema", JSON),
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
    type_: DataType

    class Config:
        fields = {
            "type_": "type"
        }


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

class InputType(Enum):
    CSV = "csv"


class InputSpec(BaseModel):
    type_: InputType 
    extra: Dict
    
    class Config:
        fields = {
            "type_": "type"
        }


class DataSource(BaseModel):
    uuid: Optional[UUID4]
    name: str
    path: str
    input_spec: Optional[InputSpec]
    schema_: DataSchema

    class Config:
        orm_mode=True

        fields = {
            "schema_": "schema"
        }

    @classmethod
    def from_record(cls, record):
        drec = dict(record)
        return cls(
            uuid=drec["uid"],
            name=drec["name"],
            path=drec["path"],
            input_spec=InputSpec(**json.loads(drec["input_spec"])),
            schema=DataSchema.from_record(json.loads(drec["schema"]))
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
        