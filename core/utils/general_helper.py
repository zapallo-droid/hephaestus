import uuid
import base58
import hashlib
import numpy as np
from datetime import datetime
from typing import Optional
import yaml
import logging
import json

logging.basicConfig(level=logging.INFO)

class ProjectConfig:
    def __init__(self, path: str):
        self.path = path

    def config_loader(self: str) -> dict:
        try:
            with open(self.path, 'r') as f:
                config = yaml.safe_load(f)
            return config
        except Exception as e:
            logging.error(f'Config file was not loaded due to: {e}')
            return {}

    def sources_loader(self) -> dict:
        try:
            with open(self.path, 'r') as f:
                sources = json.load(f)
            return sources
        except Exception as e:
            logging.error(f'Sources file was not loaded due to: {e}')
            return {}

def deterministic_code(text:str, max_length:Optional[int]=16) -> str:
    code = hashlib.sha256(text.encode('utf-8')).digest()
    code = base58.b58encode(code[:max_length]).decode('utf-8')
    return code

def random_code(max_length:Optional[int]=8) -> str:
    code = uuid.uuid4().int
    code = format(code, 'x')[:max_length]
    return code

def json_cleaner(record):

    cleaned_record = {}

    for k, v in record.items():
        if isinstance(v, datetime):
            cleaned_record[k] = v.isoformat()  # Convert datetime to string
        elif isinstance(v, float) and np.isnan(v):
            cleaned_record[k] = None  # Convert NaN to None
        else:
            cleaned_record[k] = v  # Keep other values unchanged
    return cleaned_record