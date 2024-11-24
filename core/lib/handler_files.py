import requests
import logging
import os
import csv
import gzip
from typing import Any, Union
from core.lib.handler_bucket import BucketHandler
from core.lib.jobs import Task


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
    def __init__(self, job_id:str, name:str, data_source:str, config:dict, path:str):
        self.config = config
        self.path = path
        self.name = name
        self.job_id = job_id
        self.pipeline_code = config.get("pipeline_code")
        self.source_code = config.get("source_code")
        self.data_source = data_source
        self.params = None
        self.headers = None
        self.timeout = None
        self.location = self.config.get('location', {})
        self.url_base = self.location

        self.records_processed = None
        self.task_image = None
        self.task_image_status = None
        self.location_status = None
        self.data = None
        self.job_timestamp = None
        self.task_exception = None

        super().__init__(job_id=self.job_id, name=self.name, pipeline_code=self.pipeline_code,
                         source_code=self.source_code, location=self.location, task_type_code='ttc001')

    def run(self, job):
        logging.info(f'Working on file {self.data_source}')

        try:
            self.job_timestamp = job.start_time

            data_caller = FileCall(self.location)
            self.data = data_caller.get_data()
            self.location_status = data_caller.response.status_code

            file_name = f"{self.config.get('pipeline_code', '')}_{self.source_code}"
            folder = f'raw/{self.job_id}'
            file_path = os.path.join(self.path, folder)

            logging.info(f'Exporting {self.data_source} to {file_path}')

            self.task_image = os.path.join(file_path, f'{file_name}.bin.gz')
            self.task_image_status = 'complete'
            self.records_processed = len(self.data)

            os.makedirs(file_path, exist_ok=True)

            with gzip.open(self.task_image, 'wb') as file:
                file.write(self.data)

        except Exception as e:
            self.task_exception = e
            self.task_image_status = 'failed'
