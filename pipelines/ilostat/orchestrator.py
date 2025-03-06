import os
import sys
import time
import random
from tqdm import tqdm
from core.model.jobs import Job
from core.utils.general_helper import ProjectConfig
from pipelines.ilostat.isco.src.orchestrator import ISCO


class IlostatPipeline(Job):
    def __init__(self, system_path:str, cosmos_path:str, db_config:dict):

        # - Definitions
        self.system_path = system_path

        # JOB Config and Init
        # - Reading Configuration File
        self.config_data = ProjectConfig(path=os.path.join(self.system_path,
                                                           'pipelines/ilostat',
                                                           'config/config.yaml')).config_loader()
        # - Parent Class Initialization
        super().__init__(
            name=self.config_data.get('job_name'),
            app_code=self.config_data.get('app_code'),
            db_config=db_config,
            cosmos_path=cosmos_path
        )

    def add_tasks(self):
        # ISCO (Roles)
        isco_pipe = ISCO(system_path=self.system_path, cosmos_path=self.cosmos_path,
                         job_id=self.job_id, db_config=self.db_config,
                         pipeline_code=self.config_data.get('ISCO_pipeline_code'))

        for task in isco_pipe.get_tasks():
            self.add_task(task)





