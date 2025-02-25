import os
import sys
from core.utils.general_helper import ProjectConfig
from core.model.jobs import Job
from pipelines.ilostat.isco.src.extract import ISCOOExtract


class IlostatPipeline(Job):
    def __init__(self, system_path:str, cosmos_path:str, frameworks_path:str, daedalus_config:dict):

        # JOB Config and Init
        # - Reading Configuration File
        self.config_data = ProjectConfig(path=os.path.join(system_path,
                                                           'pipelines/ilostat',
                                                           'config/config.yaml')).config_loader()
        # - Parent Class Initialization
        super().__init__(
            name=self.config_data.get('job_name'),
            app_code=self.config_data.get('app_code')
        )

        # - Definitions
        self.system_path = system_path
        self.cosmos_path = cosmos_path
        self.frameworks_path = frameworks_path
        self.daedalus_config = daedalus_config

        # -- Loading Other frameworks resources
        sys.path.append(frameworks_path)
        from daedalus.core.lib.model_operations import Job as JobORM, Task as TaskORM
        self.JobORM, self.TaskORM = JobORM, TaskORM

    def add_extract_task(self):
        # ISCO (Roles)
        roles_extract = ISCOOExtract(system_path=self.system_path, cosmos_path=self.cosmos_path,
                                     frameworks_path=self.frameworks_path, job_id=self.job_id)
        for task in roles_extract.tasks_definition():
            self.add_task(task)




