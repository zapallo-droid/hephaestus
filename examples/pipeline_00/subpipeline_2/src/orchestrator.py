import os
from tqdm import tqdm
from hephaestus.core import TaskManager
from hephaestus.core import ConfigSources
from hephaestus.core.model.elysium.elysium import ElysiumLoad
## INCLUDE MODEL CLASSES
## INCLUDE PIPELINE SPECIFIC JOBS CLASSES


class TaskManagerName(TaskManager):
    def __init__(self, system_path: str, cosmos_path: str, job_id: str, pipeline_code: str, db_config: dict):
        super().__init__(system_path=system_path,
                         cosmos_path=cosmos_path,
                         job_id=job_id,
                         db_config=db_config,
                         pipeline_code=pipeline_code)

        self.tasks = []

        # JOB Config and Init from DB
        self.config_data = ConfigSources(db_config=self.db_config).config(pipeline_code=self.pipeline_code)

    def get_tasks(self) -> list:

        # SOURCE TYPE DECLARATION
        files_data_sources = [record for record in self.config_data if record.get('location_type') == 'API']

        if files_data_sources:
            for data_source in tqdm(files_data_sources, desc='Adding Files Data Tasks'):
                # EXTRACT
                task = None #INCLUDE EXTRACT CLASS
                self.tasks.append(task)

                # TRANSFORM
                task = None #INCLUDE TRANSFORM CLASS
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
                                   model=None) #INCLUDE MODEL CLASSES

                self.tasks.append(task)

        return self.tasks

