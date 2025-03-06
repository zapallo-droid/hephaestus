import os
import sys
import time
import random
from sqlalchemy import select
from core.utils.sql_helper import DBSession, ConfigSources
from tqdm import tqdm
from core.model.elysium.resources.role import Role as RoleORM
from core.model.elysium.elysium import ElysiumLoad
from core.model.jobs import TaskManager
from core.utils.handler_files import FilesExtractor
from pipelines.ilostat.isco.src.transform import ISCOTransform


class ISCO(TaskManager):
    def __init__(self, system_path: str, cosmos_path: str, job_id: str, db_config:dict, pipeline_code:str):
        super().__init__(system_path=system_path,
                         cosmos_path=cosmos_path,
                         job_id=job_id,
                         db_config=db_config,
                         pipeline_code=pipeline_code)

        self.tasks = []

        # JOB Config and Init from DB
        self.config_data = ConfigSources(db_config=self.db_config).config(pipeline_code=self.pipeline_code)

    def get_tasks(self):
       # 1st Segment: Files Tasks
        files_data_sources = [record for record in self.config_data if record.get('location_type') == 'FILE']

        if files_data_sources:
            for data_source in tqdm(files_data_sources, desc='Adding Files Data Tasks'):
                # Extraction
                task = FilesExtractor(name=f"File {data_source.get('source_name')} extraction",
                                      data_source=data_source.get('source_name'),
                                      config=data_source,
                                      bucket_path=self.cosmos_path,
                                      job_id=self.job_id
                                      )

                self.tasks.append(task)

                # Transform
                task = ISCOTransform(job_id=self.job_id,
                                     name=f"File {data_source.get('source_name')} transformation",
                                     config=data_source,
                                     bucket_path=self.cosmos_path)
                self.tasks.append(task)


                # Load
                location = os.path.join(self.cosmos_path,
                                        "transformed",
                                        str(self.job_id),
                                        f"{str(data_source.get('pipeline_code'))}_{str(data_source.get('source_code'))}.json.gz")

                task = ElysiumLoad(job_id=self.job_id,
                                   name=f"File {data_source.get('source_name')} load",
                                   config=data_source,
                                   location=location,
                                   db_config=self.db_config,
                                   model=RoleORM)

                self.tasks.append(task)

        return self.tasks




