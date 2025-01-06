import os
import requests
import numpy as np
import logging
from tqdm import tqdm
from typing import Any, Optional
from core.utils.handler_bucket import BucketHandler
from core.model.jobs import Task


logging.basicConfig(level=logging.INFO)

class APIClientCall:
    def __init__(self, url_base: str, headers: Optional[dict[str, str]]=None, timeout: Optional[int]=30):
        self.url_base = url_base
        self.headers = headers or {}
        self.timeout = timeout
        self.response = None

    def get_data(self, endpoint: str, params: dict[str, Any]=None) -> dict:
        """
        Fetch data from API endpoint trough get method
        :param endpoint: str: API endpoint
        :param params: Dict: parameters of the call
        :return: Dict: json if exists in response
        """

        url = f'{self.url_base}/{endpoint}'
        try:
            self.response = requests.get(url, headers=self.headers, params=params, timeout=self.timeout)
            self.response.raise_for_status()
            try:
                return self.response.json()
            except ValueError as e:
                logging.error(f'Decoding failed from {url}: {str(e)}')
                return {}
        except requests.exceptions.RequestException as e:
            logging.error(f'Error {str(e)} raised when fetching data form {url}')
            return {}


class APIIteratorCall:
    def __init__(self, url_base: str, endpoint: str, headers: Optional[dict[str, str]] = None,
                 timeout: Optional[int] = 30, params: Optional[dict[str, Any]] = None):
        self.url_base: str = url_base
        self.endpoint: str = endpoint
        self.headers: dict[str, str] = headers or {}
        self.timeout: int = timeout
        self.params: dict[str, Any] = params or {}
        self.extracted_data: list = []
        self.total_records: int = 0
        self.response = []

    def get_data(self, partial_write:Optional[bool]=False, params:Optional[dict]=None) -> dict:
        # Environment
        api_call = APIClientCall(url_base=self.url_base,
                                  headers=self.headers,
                                  timeout=self.timeout)
        # First Call
        api_ping = api_call.get_data(endpoint=self.endpoint, params=self.params)
        self.response.extend([{'call':0,'response':api_call.response.status_code}])

        # Pagination
        try:
            if 'total' in api_ping.get('result').keys():
                total_records = api_ping.get('result').get('total')
                self.total_records = total_records
            else:
                raise ValueError('Unexpected response, result or total keys missing')

            if 'limit' in self.params.keys():
                calls_to_handle = int(np.ceil(total_records / self.params.get('limit')))
            else:
                raise ValueError('Unexpected response, limit key missing in params')

            for i in tqdm(list(range(0, calls_to_handle)), desc='Handling data'):
                self.params['offset'] = i * self.params.get('limit')

                data_temp = api_call.get_data(endpoint=self.endpoint, params=self.params)
                self.response.extend([{'call':i+1,'response':api_call.response.status_code}])

                if 'result' in data_temp.keys() and 'records' in data_temp.get('result').keys():
                    data_temp = data_temp.get('result').get('records')
                    self.extracted_data.extend(data_temp)

                    if partial_write:
                        BucketHandler(path=params.get('path')).exporter(data=data_temp,
                                                               file_name=params.get('file_name'),
                                                               folder=params.get('folder'),
                                                               mode='at')

                else:
                    raise ValueError('Unexpected response, result or records keys missing')

                #time.sleep(random.uniform((80/60), (100/60)))

            return self

        except ValueError as e:
            logging.error(f'Decoding failed from {self.url} and {self.endpoint}: {str(e)}')
            return {}

    def consistency_check(self) -> bool:
        if not hasattr(self, 'extracted_data'):
            return False
        else:
            return self.total_records ==len(self.extracted_data)


class APIExtractor(Task):
    def __init__(self, job_id:str, name:str, data_source:str, config:dict, path:str):
        self.config = config
        self.path = path
        self.name = name
        self.job_id = job_id
        self.pipeline_code = config.get("pipeline_code")
        self.source_code = config.get("source_code")
        self.url_base = self.config.get('location', {})
        self.data_source = data_source
        self.endpoint = self.config.get('location_endpoint', {})
        self.params = self.config.get('params', dict)
        self.headers = self.config.get('headers', {})
        self.timeout = self.config.get('timeout', 50)
        self.location = f"{self.url_base}{self.endpoint}?resource_id={self.params.get('resource_id')}"

        self.records_processed = None
        self.task_image = None
        self.task_image_status = None
        self.location_status = None
        self.data = None
        self.job_timestamp = None
        self.task_exception = None

        super().__init__(job_id=self.job_id, name=self.name, pipeline_code=self.pipeline_code,
                         source_code=self.source_code, location=self.location, task_type_code='E')

    def run(self, job):
        logging.info(f'Working on: {self.data_source}')

        try:
            self.job_timestamp = job.start_time

            file_name = f"{self.config.get('pipeline_code', '')}_{self.source_code}"
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




