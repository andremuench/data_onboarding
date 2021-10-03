from typing import Callable, Dict
from .model import DataSchema, DataType
import pandas as pd

class ValidationError(Exception):
    pass

def base_validate(v, _type):
    if isinstance(v, _type):
        return True
    elif isinstance(v, str):
        try:
            return _type(v)
        except ValueError:
            raise ValidationError

def validate_str(v):
    if isinstance(v, str):
        return v
    elif isinstance(v, bytes):
        v:bytes 
        return v.decode()
    else:
        raise ValidationError

def dummy_validate(v):
    return v

validate_int = lambda x: base_validate(x, int)
validate_float = lambda x: base_validate(x, float)
validate_bit = lambda x: base_validate(x, bool)


validator_map:Dict[DataType, Callable] = {
    DataType.STRING: validate_str,
    DataType.INTEGER: validate_int,
    DataType.FLOAT: validate_float,
    DataType.BIT: validate_bit,
    DataType.DATE: dummy_validate,
    DataType.DATETIME: dummy_validate
}

def validate_column_list(df: pd.DataFrame, schema: DataSchema):
    try:
        assert set([f.name for f in schema.fields]).issubset(df.columns)
    except AssertionError:
        raise ValidationError

def validate(df: pd.DataFrame, schema: DataSchema):
    validate_column_list(df, schema)
    for rec in df.itertuples(index=False):
        for f in schema.fields:
            try:
                val = getattr(rec, f.name)
                validator = validator_map[f.type_]
                validator(val)
            except AttributeError:
                raise ValidationError
    