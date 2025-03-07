import os
import sys
from core.utils.general_helper import ProjectConfig
from core.model.jobs import Job
from core.model.elysium.model_data_ops import Job as JobORM, Task as TaskORM
from pipelines.geopolitical_entities.iso_3166_1.src.orchestrator import ISOCountries
from pipelines.geopolitical_entities.iso_3166_2.src.orchestrator import ISOSubdivisions


class ISOGeopoliticalEntities(Job):
    def __init__(self, system_path:str, cosmos_path:str, db_config:dict):

        # JOB Config and Init
        # - Reading Configuration File
        self.config_data = ProjectConfig(path=os.path.join(system_path,
                                                           'pipelines/geopolitical_entities',
                                                           'config/config.yaml')).config_loader()
        # - Parent Class Initialization
        super().__init__(
            name=self.config_data.get('job_name'),
            app_code=self.config_data.get('app_code'),
            db_config=db_config,
            cosmos_path=cosmos_path
        )

        # - Definitions
        self.system_path = system_path
        self.JobORM, self.TaskORM = JobORM, TaskORM

    def add_tasks(self):

        # ISO Countries Codes
        countries_extract = ISOCountries(system_path=self.system_path, cosmos_path=self.cosmos_path,
                                         db_config=self.db_config, job_id=self.job_id,
                                         pipeline_code=self.config_data.get('countries_pipeline_code'))
        for task in countries_extract.get_tasks():
            self.add_task(task)

        # ISO States or Provinces Codes
        subdivisions_extract = ISOSubdivisions(system_path=self.system_path, cosmos_path=self.cosmos_path,
                                               db_config=self.db_config, job_id=self.job_id,
                                               pipeline_code=self.config_data.get('subdivisions_pipeline_code'))
        for task in subdivisions_extract.get_tasks():
            self.add_task(task)





