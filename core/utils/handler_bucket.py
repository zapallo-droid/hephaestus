import os
import json
import gzip
import logging
from typing import Optional

logging.basicConfig(level=logging.INFO)

class BucketHandler:
    def __init__(self, path: str):
        self.path = path

    def exporter(self, data: dict, file_name: str , folder: Optional[str] = None, mode: Optional[str] = None):

        logging.info(f'Writing {file_name} in the bucket')

        if folder is None:
            file_path = os.path.join(self.path)
        else:
            file_path = os.path.join(self.path, folder)

        exp_mode = mode if mode else 'wt'

        os.makedirs(file_path, exist_ok=True)

        #try:
        #    with gzip.open(os.path.join(file_path, f'{file_name}.json.gz'), mode=exp_mode) as f:
        #        json.dump(data, f, indent=4)
        try:
            with gzip.open(os.path.join(file_path, f'{file_name}.json.gz'), mode=exp_mode) as f:
                for item in data:
                    f.write(json.dumps(item) + '\n') # To allow partial json writing
            logging.info(f'File {file_name}.json.gz exported to: {file_path}')

        except Exception as e:
            logging.error(f'The file {file_name} was not exported to {file_path} due to: {str(e)}')

    def importer(self, file_name: str, folder: Optional[str] = None) -> list:

        file_path = os.path.join(self.path, folder or "", f"{file_name}.json.gz")

        try:
            logging.info(f'Reading {file_name} from the bucket')

            with gzip.open(file_path, 'rt', encoding="utf-8") as f:
                data = [json.loads(line) for line in f]

            logging.info(f'File {file_name}.json.gz successfully read from: {file_path}')
            return data

        except Exception as e:
            logging.error(f'An error occurred while reading {file_name} from {file_path}: {str(e)}')
            raise
