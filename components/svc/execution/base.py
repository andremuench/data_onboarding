from typing import Callable, Dict

import pandas as pd
from ..model import InputSpec, InputType
from dataclasses import dataclass
from abc import abstractmethod

@dataclass
class Status:
    ready: bool
    state: str

class AbstractExecutionClient:

    @abstractmethod
    def ingest(self, path, schema) -> str: 
        pass 

    @abstractmethod
    def get_status(self, id) -> Status:
        pass
