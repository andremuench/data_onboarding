import os
import yaml
from .model import Config

def get_config_path():
    return os.environ.get("DATALOADER_CFG", "dl.yaml")

def get_config() -> Config:
    with open(get_config_path(), "rb") as f:
        data = yaml.load(f)
    return Config(**data)
