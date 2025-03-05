import os
from tqdm import tqdm
from core.model.jobs import TaskManager
from core.utils.sql_helper import ConfigSources
from core.utils.handler_files import FilesExtractor
from core.model.elysium.elysium import ElysiumLoad
from core.model.elysium.resources.geopolitical_entities import GeopoliticalSubEntity
from pipelines.geopolitical_entities.iso_3166_2.src.transform import GeopoliticalSubEntitiesTransform

import logging

class ISOSubdivisions(TaskManager):
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

        files_data_sources = [record for record in self.config_data if record.get('location_type') == 'FILE']

        if files_data_sources:
            for data_source in tqdm(files_data_sources, desc='Adding Files Data Tasks'):
                logging.info(f'Adding Files Data Task: {data_source["source_name"]}')

                # EXTRACT
                task = FilesExtractor(name=f"File {data_source.get('source_name')} extraction",
                                      data_source=data_source.get('source_name'),
                                      config=data_source,
                                      bucket_path=self.cosmos_path,
                                      job_id=self.job_id
                                      )
                self.tasks.append(task)

                # TRANSFORM
                task = GeopoliticalSubEntitiesTransform(job_id=self.job_id,
                                                        name=f"API {data_source.get('source_name')} extraction",
                                                        config=data_source,
                                                        bucket_path=self.cosmos_path,
                                                        db_config=self.db_config
                                                       )
                self.tasks.append(task)

                # LOAD
                location = os.path.join(self.cosmos_path,
                                        "transformed",
                                        str(self.job_id),
                                        f"{str(data_source.get('pipeline_code'))}_{str(data_source.get('source_code'))}.json.gz")

                task = ElysiumLoad(job_id=self.job_id,
                                   name=f"File {data_source.get('source_name')} load",
                                   config=data_source,
                                   location=location,
                                   db_config=self.db_config,
                                   model=GeopoliticalSubEntity)

                self.tasks.append(task)

        return self.tasks