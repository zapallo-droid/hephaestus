import os
import time
import random
from tqdm import tqdm
from pipelines.arg_parliament.core.handler_api import APIExtractor
from core.lib.helper import ProjectConfig
from core.lib.handler_files import FilesExtractor


class LowerChamberExtract:
    def __init__(self, system_path: str, cosmos_path: str, frameworks_path: str, job_id: str):
        self.system_path = system_path
        self.cosmos_path = cosmos_path
        self.frameworks_path = frameworks_path
        self.job_id = job_id
        self.tasks = []

        # JOB Config and Init
        # - Reading Configuration File
        self.config_data = ProjectConfig(path=os.path.join(self.system_path,
                                                           'pipelines/arg_parliament/lower_chamber',
                                                           'config/config.yaml')).config_loader()

        self.pipeline_name = self.config_data.get('pipeline_name')

    def tasks_definition(self):
        # 1st Segment: API Tasks
        api_data_sources = self.config_data.get('apis', dict)

        if api_data_sources:
            api_data_sources = api_data_sources.get('endpoints', dict)
            for data_source in tqdm(api_data_sources.keys(), desc='Adding API endpoints Tasks'):
                task = APIExtractor(name=f"API {data_source} extraction",
                                    data_source=data_source,
                                    config=self.config_data,
                                    path=self.cosmos_path,
                                    job_id=self.job_id
                                    )
                self.tasks.append(task)
                time.sleep(random.uniform(1, 3))

        # 2nd Segment: Files Tasks
        files_data_sources = self.config_data.get('files', dict)
        if files_data_sources:
            for data_source in tqdm(files_data_sources.keys(), desc='Adding Files Data Tasks'):
                task = FilesExtractor(name=f"File {data_source} extraction",
                                      data_source=data_source,
                                      config=self.config_data,
                                      path=self.cosmos_path,
                                      job_id=self.job_id
                                      )

                self.tasks.append(task)
                time.sleep(random.uniform(1, 3))

        return self.tasks