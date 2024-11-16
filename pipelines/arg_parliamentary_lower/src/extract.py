#%%
import os
import csv
import json
import logging
from core.lib.handler_api import APIIteratorCall
from core.lib.handler_files import FileCall
from core.lib.handler_bucket import BucketHandler
from core.lib.jobs import Task


class APIExtractor(Task):
    def __init__(self, job_id:str, name:str, data_source:str, config:dict, path:str):
        self.config = config
        self.path = path
        self.name = name
        self.job_id = job_id
        self.pipeline_code = config.get("pipeline_code")
        self.url_base = self.config.get('apis', {}).get('url_base', str)
        self.data_source = data_source
        self.endpoint = self.config.get('apis', {}).get('endpoints', {}).get(self.data_source).get('endpoint', str)
        self.params = self.config.get('apis', {}).get('endpoints', {}).get(self.data_source).get('params', dict)
        self.headers = self.config.get('apis', {}).get('headers', {})
        self.timeout = self.config.get('apis', {}).get('timeout', 50)
        self.location = f"{self.url_base}{self.endpoint}?resource_id={self.params.get('resource_id')}"

        self.records_processed = None
        self.task_image = None
        self.task_image_status = None
        self.location_status = None
        self.data = None
        self.job_timestamp = None

        super().__init__(job_id=self.job_id, name=self.name, pipeline_code=self.pipeline_code, location=self.location)

    def run(self, job):
        logging.info(f'Working on: {self.data_source}')

        try:
            self.job_timestamp = job.start_time

            file_name = f"{self.config.get('pipeline_name', '')}_{self.data_source}"
            folder = f'raw/{self.job_id}'
            self.task_image = os.path.join(self.path, folder, f'{file_name}.json.gz')

            exporter_dict = {'path': self.path,
                             'file_name':file_name,
                             'folder':folder}

            iterator_caller = APIIteratorCall(url_base=self.url_base,
                                              endpoint=self.endpoint,
                                              params=self.params,
                                              headers=self.headers,
                                              timeout=self.timeout
                                              )

            self.data = iterator_caller.get_data(partial_write=True, params=exporter_dict).extracted_data
            self.records_processed = len(self.data)

            response_status = iterator_caller.response # Considering that the APIIterator return a dict here
            self.location_status = {resp.get('call'): resp.get('response') for resp in response_status}
            max_call = max(self.location_status)
            self.location_status = self.location_status.get(max_call)

            logging.info(f'The extraction for {self.data_source} succeed: {iterator_caller.consistency_check()}')

            if iterator_caller.consistency_check():
                logging.info(f'Exporting {self.data_source} to {self.path}')
                self.task_image_status = 'complete'
            else:
                self.task_image_status = 'failed'
                logging.error(f'The consistency check failed when working on: {self.data_source}')

        except Exception as e:
            self.task_exception = e
            logging.error(f'The run failed: Exception {str(e)}')


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

            if '.json' in self.location:
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

            file_name = f"{self.config.get('pipeline_name', '')}_{self.data_source}"
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












