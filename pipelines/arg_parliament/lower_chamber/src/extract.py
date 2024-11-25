import os
import time
import random
from tqdm import tqdm
from pipelines.arg_parliament.core.handler_api import APIExtractor
from core.utils.helper import ProjectConfig
from core.utils.handler_files import FilesExtractor


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
                                                           'config/sources.json')).sources_loader()

    def tasks_definition(self):
        # 1st Segment: API Tasks
        api_data_sources = [record for record in self.config_data if record.get('location_type') == 'API']

        if api_data_sources:
            for data_source in tqdm(api_data_sources, desc='Adding API endpoints Tasks'):
                task = APIExtractor(name=f"API {data_source.get('source_name')} extraction",
                                    data_source=data_source.get('source_name'),
                                    config=data_source,
                                    path=self.cosmos_path,
                                    job_id=self.job_id
                                    )
                self.tasks.append(task)
                time.sleep(random.uniform(1, 3))

        # 2nd Segment: Files Tasks
        files_data_sources = [record for record in self.config_data if record.get('location_type') == 'FILE']

        if files_data_sources:
            for data_source in tqdm(files_data_sources, desc='Adding Files Data Tasks'):
                task = FilesExtractor(name=f"File {data_source.get('source_name')} extraction",
                                      data_source=data_source.get('source_name'),
                                      config=data_source,
                                      path=self.cosmos_path,
                                      job_id=self.job_id
                                      )
                self.tasks.append(task)
                time.sleep(random.uniform(1, 3))

        return self.tasks