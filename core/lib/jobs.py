from datetime import datetime as dt
import psutil
import socket
import getpass
import os
import uuid
import traceback
import logging
from typing import Optional
from core.lib.handler_bucket import BucketHandler
from core.lib.sql_helper import DBSession, DBInitializer
from sqlalchemy.dialects.postgresql import insert


class Task:
    def __init__(self, job_id:str, name: str, pipeline_code: str, location:Optional[str] = None):
        self.name = name
        self.location = location
        self.location_status = None
        self.job_id = job_id
        self.pipeline_code = pipeline_code
        self.task_id = f'T-{str(uuid.uuid4())}'
        self.start_time = None
        self.end_time = None
        self.status = 'not started'
        self.duration = None
        self.records_processed = 0
        self.task_exception = None
        self.stats = {}

    def start(self):
        self.start_time = dt.now()
        self.status = 'started'

        psutil.cpu_percent(interval=1) # Priming measure (reset state)
        self.stats['job_id'] = self.job_id
        self.stats['task_id'] = self.task_id
        self.stats['pipeline_code'] = self.pipeline_code
        self.stats['name'] = self.name
        self.stats['location'] = self.location
        self.stats['memory_usage_start'] = psutil.virtual_memory().used / (1024 ** 2)
        self.stats['cpu_usage_start'] = psutil.cpu_percent(interval=1)

        logging.info(f'{self.name} {self.status}')

    def finish(self):
        self.end_time = dt.now()
        self.duration = (self.end_time - self.start_time).total_seconds()
        self.status = 'finished'

        psutil.cpu_percent(interval=1)  # Priming measure (reset state)
        self.stats['memory_usage_end'] = psutil.virtual_memory().used / (1024 ** 2)
        self.stats['cpu_usage_end'] = psutil.cpu_percent(interval=1)

        logging.info(f'{self.name} {self.status}')

    def fail(self, exception: Optional[Exception] = None, location_status:Optional[str]=None):
        self.end_time = dt.now()
        self.duration = (self.end_time - self.start_time).total_seconds()
        self.status = 'failed'

        psutil.cpu_percent(interval=1)  # Priming measure (reset state)
        self.stats['memory_usage_end'] = psutil.virtual_memory().used / (1024 ** 2)
        self.stats['cpu_usage_end'] = psutil.cpu_percent(interval=1)

        logging.info(f'Job {self.name}: {self.status}. Exception: {self.task_exception}')

    def stats_builder(self):
        self.stats['status'] = self.status
        self.stats['location_status'] = self.location_status
        self.stats['task_image'] = self.task_image
        self.stats['task_image_status'] = self.task_image_status
        self.stats['exception'] = str(self.task_exception)
        self.stats['duration'] = self.duration
        self.stats['status'] = self.status
        self.stats['records_processed'] = self.records_processed
        self.stats['memory_usage'] = max(0, self.stats['memory_usage_end'] - self.stats['memory_usage_start'])
        self.stats['cpu_usage'] = max(0, self.stats['cpu_usage_end'] - self.stats['cpu_usage_start'])

        return self.stats


class Job:
    def __init__(self, name: str, app_code:Optional[str]=None):
        self.name = name
        self.job_id = f'J-{str(uuid.uuid4())}'
        self.app_code = app_code
        self.tasks = []
        self.start_time = None
        self.end_time = None
        self.status = 'not started'
        self.duration = None
        self.job_exception = None
        self.stats = {}
        self.tasks_stats = {}

    def add_task(self, task: Task):
        self.tasks.append(task)

    def start(self):
        self.start_time = dt.now()
        self.status = 'started'

        # Prime CPU measure and reset
        psutil.cpu_percent(interval=1)  # Priming measure (reset state)
        self.stats['name'] = self.name
        self.stats['app_code'] = self.app_code
        self.stats['job_id'] = self.job_id
        self.stats['name'] = self.name
        self.stats['memory_usage_start'] = psutil.virtual_memory().used / (1024 ** 2)
        self.stats['cpu_usage_start'] = psutil.cpu_percent(interval=1)

        logging.info(f'Job {self.name}: {self.status}')

    def finish(self):
        self.end_time = dt.now()
        self.status = 'finished'

        psutil.cpu_percent(interval=1)  # Priming measure (reset state)
        self.stats['memory_usage_end'] = psutil.virtual_memory().used / (1024 ** 2)
        self.stats['cpu_usage_end'] = psutil.cpu_percent(interval=1)

        logging.info(f'Job {self.name}: {self.status}')

    def fail(self):
        self.end_time = dt.now()
        self.status = 'failed'

        psutil.cpu_percent(interval=1)  # Priming measure (reset state)
        self.stats['memory_usage_end'] = psutil.virtual_memory().used / (1024 ** 2)
        self.stats['cpu_usage_end'] = psutil.cpu_percent(interval=1)

        logging.info(f'Job {self.name}: {self.status}. Exception: {str(self.job_exception)}')

    def stats_builder(self):
        self.tasks_stats = [task.stats_builder() for task in self.tasks]

        self.duration = (self.end_time - self.start_time).total_seconds() if self.end_time and self.start_time else None
        self.stats['status'] = self.status
        self.stats['exception'] = str(self.job_exception)
        self.stats['duration'] = self.duration
        self.stats['memory_usage'] =  max(0, self.stats['memory_usage_end'] - self.stats['memory_usage_start'])  # Avoid negative usage
        self.stats['cpu_usage'] = max(0, self.stats['cpu_usage_end'] - self.stats['cpu_usage_start'])  # Avoid negative CPU usage
        self.stats['host_name'] = socket.gethostname()
        self.stats['execution_user'] = getpass.getuser()
        self.stats['process_id'] = os.getpid()
        self.stats['number_of_tasks'] = len(self.tasks)

        return self.stats, self.tasks_stats

    def execute(self):
        self.start()
        try:
            for task in self.tasks:
                try:
                    task.start()
                    task.run(self)
                    task.finish()
                except Exception as e:
                    task.fail(e)
            self.finish()
        except Exception as e:
            self.job_exception = e
            self.fail()
        finally:
            self.stats_builder()

    def run_job(self):
        # Adding Job Tasks
        self.add_extract_task()

        # Job Execution
        self.execute()

        job_stats, tasks_stats = self.stats_builder()

        # STATS
        # - Stats Data To Cosmos
        logging.info("Writing Job {job.get('job_id')} stats" in {os.path.join(self.cosmos_path, 'stats/')})

        for stats in [(job_stats, 'J'), (tasks_stats, 'T')]:
            BucketHandler(path=self.cosmos_path).exporter(data=stats[0],
                                                          file_name=f'{stats[1]}_{self.job_id}',
                                                          folder='jobs/',
                                                          mode='wt')

        # - Stats Data To Daedalus
        logging.info("Sending Job {job.get('job_id')} stats to Daedalus DB")
        db_session = DBSession(**self.daedalus_config)
        session = db_session.create()

        try:
            # Getting typical stats file
            job_stats = BucketHandler(path=self.cosmos_path).importer(file_name=f'J_{self.job_id}',
                                                                      folder='jobs/')

            tasks_stats = BucketHandler(path=self.cosmos_path).importer(file_name=f'T_{self.job_id}',
                                                                        folder='jobs/')

            # -- Loading data to DB
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
                'app_code': job_stats.get('app_code')
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





