import requests
import logging
import os
import gzip
from typing import Any, Union
from hephaestus_pframe.core.model.jobs import Task


class FileCall:
    def __init__(self, url: str):
        self.url = url
        self.response = None

    def get_data(self) -> Union[dict[str, Any], list[dict[str, Any]]]:
        try:
            self.response = requests.get(self.url)
            self.response.raise_for_status()
            try:
                return self.response.content
            except ValueError as e:
                logging.error(f'Decoding failed from {self.url}: {str(e)}')
                return {}
        except requests.exceptions.RequestException as e:
            logging.error(f'Error {str(e)} raised when fetching data form {self.url}')
            return {}


class FilesExtractor(Task):
    def __init__(self, job_id:str, name:str, data_source:str, config:dict, bucket_path:str):

        super().__init__(job_id=job_id,
                         name=name,
                         pipeline_code=config.get("pipeline_code"),
                         source_code=config.get("source_code"),
                         location=config.get("location"),
                         task_type_code='E')

        self.config = config
        self.bucket_path = bucket_path
        self.data_source = data_source
        self.params = None
        self.headers = None
        self.timeout = None
        self.url_base = self.location
        self.data = None

    def run(self):
        logging.info(f'Working on file {self.data_source}')

        try:
            data_caller = FileCall(self.location)
            self.data = data_caller.get_data()
            self.location_status = data_caller.response.status_code

            file_name = f"{self.source_code}"
            folder = f'raw/{self.job_id}'
            file_path = os.path.join(self.bucket_path, folder)

            logging.info(f'Exporting {self.data_source} to {file_path}')

            self.task_image = os.path.join(file_path, f'{file_name}.bin.gz')
            self.records_processed = len(self.data) if self.data else 0

            os.makedirs(file_path, exist_ok=True)

            with gzip.open(self.task_image, 'wb') as file:
                file.write(self.data)

        except Exception as e:
            self.fail(e)
