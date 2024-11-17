import requests
import logging
import os
import csv
import json
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
        self.data_source = data_source
        self.params = None
        self.headers = None
        self.timeout = None
        self.location = (self.config.get('files', {}).get(self.data_source).get('location') or
                         self.config.get('files', {}).get(self.data_source).get('secondary_location'))
        self.url_base = self.location

        self.records_processed = None
        self.task_image = None
        self.task_image_status = None
        self.location_status = None
        self.data = None
        self.job_timestamp = None

        super().__init__(job_id=self.job_id, name=self.name, pipeline_code=self.pipeline_code, location=self.location)

    def run(self, job):
        logging.info(f'Working on file {self.data_source}')

        try:
            self.job_timestamp = job.start_time

            if 'json' in self.location:
                data_caller = FileCall(self.location)
                self.data = json.loads(data_caller.get_data())
                self.location_status = data_caller.response.status_code

            else:
                data_caller = FileCall(self.location)
                self.data = data_caller.get_data().decode('utf-8').splitlines()
                self.location_status = data_caller.response.status_code
                self.data = csv.DictReader(self.data)
                self.data = list(self.data)
                self.data = json.dumps(self.data, indent=4)

            logging.info(f'Exporting {self.data_source} to {self.path}')

            file_name = f"{self.config.get('pipeline_code', '')}_{self.data_source}"
            folder = f'raw/{self.job_id}'

            self.task_image = os.path.join(self.path, folder, f'{file_name}.json.gz')
            self.task_image_status = 'complete'
            self.records_processed = len(self.data)

            BucketHandler(path=self.path).exporter(data=self.data,
                                                   file_name=file_name,
                                                   folder=folder,
                                                   mode='wt')

        except Exception as e:
            self.task_exception = e
