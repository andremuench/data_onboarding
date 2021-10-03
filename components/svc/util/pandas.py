from dataclasses import dataclass
from typing import Callable, Dict
from ..model import InputSpec, InputType
import pandas as pd

@dataclass
class PandasConf:
    load_callable: Callable
    kwargs_mapping: Dict

    def get_pandas_kwargs(self, input_spec):
        pd_kwargs = dict()
        for k,v in self.kwargs_mapping.items():
            try:
                pd_kwargs[k] = input_spec.extra[v]
            except KeyError:
                pass
        return pd_kwargs

    def load(self, data, input_spec: InputSpec) -> pd.DataFrame:
        return self.load_callable(data, **self.get_pandas_kwargs(input_spec))


pandas_loader = {
    InputType.CSV: PandasConf(
        load_callable=pd.read_csv,
        kwargs_mapping={
            "delimiter": "delimiter",
            "escapechar": "escapechar"
        })
}

def load_data(data, input_spec: InputSpec):
    try:
        loader = pandas_loader[input_spec.type_]
    except KeyError:
        raise ValueError(f"No such loader found for {str(input_spec.type_)}")
    return loader.load(data, input_spec)