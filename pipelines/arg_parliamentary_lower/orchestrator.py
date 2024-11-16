# Libraries
import os
import time
import random
import logging
import sys
import pandas as pd
import traceback
from tqdm import tqdm
from sqlalchemy.dialects.postgresql import insert
from core.lib.helper import ProjectConfig
from core.lib.jobs import Job
from core.lib.handler_bucket import BucketHandler
from core.lib.sql_helper import DBSession, DBInitializer
from pipelines.arg_parliamentary_lower.src.extract import APIExtractor, FilesExtractor


class ArgParliamentLowerJob(Job):
    def __init__(self, system_path:str, cosmos_path:str, frameworks_path:str, daedalus_config:dict):

        # JOB Config and Init
        # - Reading Configuration File
        self.config_data = ProjectConfig(path=os.path.join(system_path,
                                                           'pipelines/arg_parliamentary_lower',
                                                           'config/config.yaml')).config_loader()
        # - Parent Class Initialization
        super().__init__(
            name=self.config_data.get('job_name'),
            app_code=self.config_data.get('app_code'),
            app_name=self.config_data.get('app_name'),
            pipeline_code=self.config_data.get('pipeline_code'),
            pipeline_name=self.config_data.get('pipeline_name')
        )

        # - Definitions
        self.system_path = system_path
        self.cosmos_path = cosmos_path
        self.frameworks_path = frameworks_path
        self.daedalus_config = daedalus_config

        # -- Loading Other frameworks resources
        sys.path.append(frameworks_path)
        from daedalus.core.lib.model_operations import (App as AppORM, Pipeline as PipelineORM, Job as JobORM,
                                                        Task as TaskORM)
        self.AppORM, self.PipelineORM, self.JobORM, self.TaskORM = AppORM, PipelineORM, JobORM, TaskORM


    def add_extract_task(self):
        # 1st Segment: API Tasks
        api_data_sources = self.config_data.get('apis', dict).get('endpoints', dict)

        for data_source in tqdm(api_data_sources.keys(), desc='Adding API endpoints Tasks'):
            self.add_task(
                APIExtractor(name=f"API {data_source} extraction",
                             data_source=data_source,
                             config=self.config_data,
                             path=self.cosmos_path,
                             job_id=self.job_id
                             ))

            time.sleep(random.uniform(1, 3))

        # 2nd Segment: Files Tasks
        files_data_sources = self.config_data.get('files', dict)
        for data_source in tqdm(files_data_sources.keys(), desc='Adding Files Data Tasks'):
            self.add_task(
                FilesExtractor(name=f"File {data_source} extraction",
                               data_source=data_source,
                               config=self.config_data,
                               path=self.cosmos_path,
                               job_id=self.job_id
                               ))

            time.sleep(random.uniform(1, 3))

    def run_job(self):

        # Adding Job Tasks
        self.add_extract_task()

        # Job Execution
        self.execute()

        job_stats, tasks_stats = self.stats_builder()

        # STATS
        # - Stats Data To Cosmos
        logging.info("Writting Job {job.get('job_id')} stats" in {os.path.join(self.cosmos_path, 'stats/')})
        BucketHandler(path=self.cosmos_path).exporter(data=job_stats,
                                                      file_name=f'J_{self.job_id}',
                                                      folder='jobs/',
                                                      mode='wt')

        BucketHandler(path=self.cosmos_path).exporter(data=tasks_stats,
                                                      file_name=f'T_{self.job_id}',
                                                      folder='jobs/',
                                                      mode='wt')

        # - Stats Data To Daedalus
        logging.info("Sending Job {job.get('job_id')} stats to Daedalus DB")
        db_session = DBSession(**self.daedalus_config)
        session = db_session.create()

        try:
            # Getting a typical jobs file
            job_stats = BucketHandler(path=self.cosmos_path).importer(file_name=f'J_{self.job_id}',
                                                                      folder='jobs/')

            tasks_stats = BucketHandler(path=self.cosmos_path).importer(file_name=f'T_{self.job_id}',
                                                                        folder='jobs/')

            # -- Initializing DB
            db_init = DBInitializer(session=db_session)
            db_init.db_init([self.AppORM, self.PipelineORM, self.JobORM, self.TaskORM])

            # -- Loading data to DB
            app_data = {'app_code': job_stats.get('app_code'), 'app_name': job_stats.get('app_name')}
            sentence = insert(self.AppORM).values(app_data).on_conflict_do_nothing(index_elements=['app_code'])
            session.execute(sentence)

            pipeline_data = {'app_code': job_stats.get('app_code'),
                             'pipeline_code': job_stats.get('pipeline_code'),
                             'pipeline_name': job_stats.get('pipeline_name')
            }
            sentence = insert(self.PipelineORM).values(pipeline_data).on_conflict_do_nothing(
                index_elements=['pipeline_code'])
            session.execute(sentence)

            job_data = {
                'job_id': job_stats.get('job_id'),
                'name': job_stats.get('name'),
                'memory_usage_start': job_stats.get('memory_usage_start'),
                'cpu_usage_start': job_stats.get('cpu_usage_start'),
                'memory_usage_end': job_stats.get('memory_usage_end'),
                'cpu_usage_end': job_stats.get('cpu_usage_end'),
                'status': job_stats.get('status'),
                'exception': job_stats.get('exception'),
                'duration': job_stats.get('duration'),
                'memory_usage': job_stats.get('memory_usage'),
                'cpu_usage': job_stats.get('cpu_usage'),
                'host_name': job_stats.get('host_name'),
                'execution_user': job_stats.get('execution_user'),
                'process_id': job_stats.get('process_id'),
                'number_of_tasks': job_stats.get('number_of_tasks'),
                'app_code': job_stats.get('app_code'),
                'app_name': job_stats.get('app_name')
            }
            sentence = insert(self.JobORM).values(job_data).on_conflict_do_nothing(index_elements=['job_id'])
            session.execute(sentence)

            for task in tasks_stats:
                sentence = insert(self.TaskORM).values(task).on_conflict_do_nothing(index_elements=['task_id'])
                session.execute(sentence)

            session.commit()

        except Exception as e:
            session.rollback()
            logging.warning(
                f"Session rollback --> Exception occurred: {e}\n"
                f"Traceback details:\n{traceback.format_exc()}"
            )

        finally:
            session.close()
