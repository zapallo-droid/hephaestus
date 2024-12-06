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