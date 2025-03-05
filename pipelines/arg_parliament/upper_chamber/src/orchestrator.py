import os
import time
import random
from tqdm import tqdm
from core.model.jobs import TaskManager
from core.utils.sql_helper import ConfigSources
from core.utils.handler_files import FilesExtractor


class ARGUpperChamber(TaskManager):
    def __init__(self, system_path: str, cosmos_path: str, job_id: str, pipeline_code: str, db_config: dict):
        super().__init__(system_path=system_path,
                         cosmos_path=cosmos_path,
                         job_id=job_id,
                         db_config=db_config,
                         pipeline_code=pipeline_code)

        self.tasks = []

        # JOB Config and Init from DB
        self.config_data = ConfigSources(db_config=self.db_config).config(pipeline_code=self.pipeline_code)

    def get_tasks(self):
        # EXTRACT
        files_data_sources = [record for record in self.config_data if record.get('location_type') == 'FILE']

        if files_data_sources:
            for data_source in tqdm(files_data_sources, desc='Adding Files Data Tasks'):
                task = FilesExtractor(name=f"File {data_source.get('source_name')} extraction",
                                      data_source=data_source.get('source_name'),
                                      config= data_source,
                                      bucket_path=self.cosmos_path,
                                      job_id=self.job_id
                                      )
                self.tasks.append(task)
                time.sleep(random.uniform(1, 3))

        return self.tasks