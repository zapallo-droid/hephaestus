import os
import requests
import numpy as np
import logging
from tqdm import tqdm
from typing import Any, Optional
from hephaestus_pframe.core.utils.handler_bucket import BucketHandler
from hephaestus_pframe.core.model.jobs import Task

logging.basicConfig(level=logging.INFO)

class APIClientCall:
    def __init__(self, url_base: str, headers: Optional[dict[str, str]]=None, timeout: Optional[int]=30):
        self.url_base = url_base
        self.headers = headers or {}
        self.timeout = timeout
        self.response = None

    def get_data(self, params: dict[str, Any]=None) -> dict:
        """
        Fetch data from API endpoint trough get method
        :param params: Dict: parameters of the call
        :return: Dict: json if exists in response
        """

        url = f'{self.url_base}'
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
    def __init__(self, url_base: str, total_attribute:str, results_attribute:str,
                 sub_results_attribute:Optional[str]=None, limit_attribute:Optional[str]=None,
                 offset_attribute:Optional[str]=None, headers: Optional[dict[str, str]] = None,
                 timeout: Optional[int] = 30, params: Optional[dict[str, Any]] = None):

        self.url_base: str = url_base
        self.total_attribute: str = total_attribute
        self.results_attribute: str = results_attribute
        self.sub_results_attribute: str = sub_results_attribute
        self.limit_attribute: str = limit_attribute
        self.offset_attribute: str = offset_attribute
        self.headers: dict[str, str] = headers or {}
        self.timeout: int = timeout
        self.params: dict[str, Any] = params or {}
        self.extracted_data: list = []
        self.total_records: int = 0
        self.response: list = []

    def get_data(self, partial_write:Optional[bool]=False, params:Optional[dict]=None) -> dict:
        # Environment
        api_call = APIClientCall(url_base=self.url_base,
                                  headers=self.headers,
                                  timeout=self.timeout)
        # First Call
        api_ping = api_call.get_data(params=self.params)
        self.response.extend([{'call':0,'response':api_call.response.status_code}])

        # Pagination
        try:
            if self.results_attribute in api_ping.keys():
                total_records = api_ping.get(self.total_attribute)
                self.total_records = total_records
            elif self.total_attribute in api_ping.get(self.results_attribute).keys():
                total_records = api_ping.get(self.results_attribute).get(self.total_attribute)
                self.total_records = total_records
            else:
                raise ValueError('Unexpected response, result or total keys missing')

            if self.limit_attribute in self.params.keys():
                calls_to_handle = int(np.ceil(total_records / self.params.get(self.limit_attribute)))
            else:
                calls_to_handle = 1 # No paginated response

            for i in tqdm(list(range(0, calls_to_handle)), desc='Handling data'):
                if self.offset_attribute:
                    self.params[self.offset_attribute] = i * self.params.get(self.limit_attribute)

                data_temp = api_call.get_data(params=self.params).get(self.results_attribute)
                self.response.extend([{'call':i+1,'response':api_call.response.status_code}])

                self.extracted_data.extend(data_temp)

                if partial_write:
                    BucketHandler(path=params.get('path')).exporter(data=data_temp,
                                                           file_name=params.get('file_name'),
                                                           folder=params.get('folder'),
                                                           mode='at')
            return self

        except ValueError as e:
            logging.error(f'Decoding failed from {self.url_base}: {str(e)}')
            return {}

    def consistency_check(self) -> bool:
        if not hasattr(self, 'extracted_data'):
            return False
        else:
            return self.total_records ==len(self.extracted_data)


class APIExtractor(Task):
    def __init__(self, job_id:str, name:str, data_source:str, config:dict, bucket_path:str, attributes:Optional[dict]=None):

        self.config = config
        self.url_base = self.config.get('location')
        self.params = self.config.get('params', {})
        self.headers = self.config.get('headers')
        self.timeout = self.config.get('timeout', 50)

        super().__init__(job_id=job_id,
                         name=name,
                         pipeline_code=config.get("pipeline_code"),
                         source_code=config.get("source_code"),
                         location=f"{self.url_base}?",
                         task_type_code='E')

        self.bucket_path = bucket_path
        self.data_source = data_source
        self.attributes = attributes
        self.data = None

    def run(self):
        logging.info(f'Working on: {self.data_source}')

        try:
            file_name = f"{self.source_code}"
            folder = f'raw/{self.job_id}'
            self.task_image = os.path.join(self.bucket_path, folder, f'{file_name}.json.gz')

            exporter_dict = {'path': self.bucket_path,
                             'file_name':file_name,
                             'folder':folder}

            iterator_caller = APIIteratorCall(url_base=self.url_base,
                                              params=self.params,
                                              headers=self.headers,
                                              timeout=self.timeout,
                                              **self.attributes
                                              )

            self.data = iterator_caller.get_data(partial_write=True, params=exporter_dict)

            if isinstance(self.data, list):
                self.records_processed = len(self.data)
            else:
                self.records_processed = None

            response_status = iterator_caller.response # Considering that the APIIterator return a dict here
            self.location_status = {resp.get('call'): resp.get('response') for resp in response_status}
            max_call = max(self.location_status)
            self.location_status = self.location_status.get(max_call)

            logging.info(f'The extraction for {self.data_source} succeed: {iterator_caller.consistency_check()}')

            if iterator_caller.consistency_check():
                logging.info(f'Exporting {self.data_source} to {self.bucket_path}')
                self.task_image_status = 'complete'
            else:
                self.task_image_status = 'failed'
                logging.error(f'The consistency check failed when working on: {self.data_source}')

        except Exception as e:
            self.fail(e)




