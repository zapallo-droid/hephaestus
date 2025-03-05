import os
import sys
from core.utils.general_helper import ProjectConfig
from core.model.jobs import Job
from core.model.elysium.model_data_ops import Job as JobORM, Task as TaskORM
from pipelines.arg_parliament.lower_chamber.src.orchestrator import ARGLowerChamber
from pipelines.arg_parliament.upper_chamber.src.orchestrator import ARGUpperChamber


class ArgParliamentPipeline(Job):
    def __init__(self, system_path:str, cosmos_path:str, db_config:dict):

        # JOB Config and Init
        # - Reading Configuration File
        self.config_data = ProjectConfig(path=os.path.join(system_path,
                                                           'pipelines/arg_parliament',
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
        # EXTRACT
        # Lower House (Deputies Chamber)
        lower_extract = ARGLowerChamber(system_path=self.system_path, cosmos_path=self.cosmos_path,
                                        db_config=self.db_config, job_id=self.job_id,
                                        pipeline_code=self.config_data.get('lower_pipeline_code'))
        for task in lower_extract.get_tasks():
            self.add_task(task)

        # Upper House (Senate)
        upper_extract = ARGUpperChamber(system_path=self.system_path, cosmos_path=self.cosmos_path,
                                        db_config=self.db_config, job_id=self.job_id,
                                        pipeline_code=self.config_data.get('upper_pipeline_code'))
        for task in upper_extract.get_tasks():
            self.add_task(task)

        # TRANSFORM



        # LOAD




