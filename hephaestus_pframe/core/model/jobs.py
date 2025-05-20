from datetime import datetime as dt
import psutil
import socket
import getpass
import os
import uuid
import traceback
import logging
from typing import Optional
from hephaestus_pframe.core.utils.handler_bucket import BucketHandler
from hephaestus_pframe.core.model.elysium.model_data_ops import Job as JobORM, Task as TaskORM
from hephaestus_pframe.core.utils.sql_helper import DB


class Task:
    STATUS_NOT_STARTED = 'not started'
    STATUS_STARTED = 'started'
    STATUS_FINISHED = 'finished'
    STATUS_FAILED = 'failed'

    def __init__(self, job_id:str, name: str, pipeline_code: str, source_code: str, task_type_code:str,
                 cosmos_path:Optional[str] = None, task_image:Optional[str] = None,
                 #task_image_status:Optional[str] = None,
                 location:Optional[str] = None):
        self.name = name
        self.location = location
        self.source_code = source_code
        self.task_type_code = task_type_code
        self.location_status = None
        self.job_id = job_id
        self.pipeline_code = pipeline_code
        self.task_id = uuid.uuid4()
        self.start_time = None
        self.end_time = None
        self.status = Task.STATUS_NOT_STARTED
        self.duration = None
        self.records_processed = 0
        self.files_processed = 0
        self.task_exception = None
        self.task_image = task_image
        self.task_image_status = Task.STATUS_NOT_STARTED if self.task_image else None
        self.cosmos_path = cosmos_path
        self.stats = {
            'job_id': self.job_id,
            'task_id': self.task_id,
            'pipeline_code': self.pipeline_code,
            'name': self.name,
            'source_code': self.source_code,
            'task_type_code': self.task_type_code,
            'location': self.location,
            'memory_usage_start': 0,
            'memory_usage_end': 0,
            'cpu_usage_start': 0,
            'cpu_usage_end': 0,
            'started_at': None,
            'ended_at': None,
            'duration': None,
            'status': self.status,
            'records_processed': 0,
            'files_processed': 0,
            'exception': None,
            'location_status': None
        }


    def start(self):
        self.start_time = dt.now()
        self.status = Task.STATUS_STARTED
        self.task_image_status = Task.STATUS_STARTED if self.task_image else None
        self.stats['started_at'] = self.start_time.isoformat() if self.start_time else None

        psutil.cpu_percent(interval=1) # Priming measure (reset state)
        self.stats['memory_usage_start'] = psutil.virtual_memory().used / (1024 ** 2)
        self.stats['cpu_usage_start'] = psutil.cpu_percent(interval=1)

        self.stats_builder()

        logging.info(f'{self.name} {self.status}')

    def finish(self):
        self.end_time = dt.now()
        self.stats['ended_at'] = self.end_time.isoformat() if self.end_time else None
        self.duration = (self.end_time - self.start_time).total_seconds()
        self.status = Task.STATUS_FINISHED
        self.task_image_status = Task.STATUS_FINISHED if self.task_image else None

        psutil.cpu_percent(interval=1)  # Priming measure (reset state)
        self.stats['memory_usage_end'] = psutil.virtual_memory().used / (1024 ** 2)
        self.stats['cpu_usage_end'] = psutil.cpu_percent(interval=1)

        logging.info(f'{self.name} {self.status}')

    def fail(self, exception: Optional[Exception] = None):
        self.task_exception = traceback.format_exc()
        self.end_time = dt.now()
        self.stats['ended_at'] = self.end_time.isoformat() if self.end_time else None
        self.duration = (self.end_time - self.start_time).total_seconds()
        self.status = Task.STATUS_FAILED
        self.task_image_status = Task.STATUS_FAILED if self.task_image else None
        self.stats['exception'] = self.task_exception or str(exception)

        psutil.cpu_percent(interval=1)  # Priming measure (reset state)
        self.stats['memory_usage_end'] = psutil.virtual_memory().used / (1024 ** 2)
        self.stats['cpu_usage_end'] = psutil.cpu_percent(interval=1)

        self.stats_builder()
        logging.info(f'Job {self.name}: {self.status}. Exception: {self.task_exception}')

    def stats_builder(self):
        self.stats['status'] = self.status
        self.stats['location'] = self.location
        self.stats['location_status'] = self.location_status
        self.stats['task_image'] = self.task_image
        self.stats['task_image_status'] = self.task_image_status
        self.stats['duration'] = self.duration
        self.stats['records_processed'] = self.records_processed
        self.stats['files_processed'] = self.files_processed
        self.stats['memory_usage'] = max(0, self.stats['memory_usage_end'] - self.stats['memory_usage_start'])
        self.stats['cpu_usage'] = max(0, self.stats['cpu_usage_end'] - self.stats['cpu_usage_start'])

        return self.stats

    def run(self):
        e = NotImplementedError('Subclasses must implement this method')
        self.fail(e)

    def bucket_imaging(self, data:list[dict], pipeline_config:dict, bucket_path:str):
        logging.info(f'Bucketing file: {self.name}')

        try:
            if data:
                file_name = f"{pipeline_config.get('pipeline_code', '')}_{self.source_code}"

                if self.task_type_code == "E":
                    folder = f'raw/{self.job_id}'
                elif self.task_type_code == "T":
                    folder = f'transformed/{self.job_id}'
                else:
                    folder = None
                    logging.error(f'Task type code should be E or T, received {self.task_type_code}')

                file_path = os.path.join(bucket_path, folder)

                logging.info(f'Exporting {self.name} to {file_path}')

                self.task_image = os.path.join(file_path, f'{file_name}.json.gz')
                self.records_processed = len(data)
                self.files_processed += 1

                bucket = BucketHandler(path=bucket_path)
                bucket.exporter(data=data,
                                file_name=file_name,
                                folder=folder,
                                partial=False)

                logging.info(f'{self.records_processed} records, successfully loaded')

            else:
                logging.warning("No data received")
                e = Exception("No data received")
                self.fail(e)
                raise e

        except Exception as e:
            self.fail(e)


    def execute(self, db):
        self.start()
        db.records_loader(model=TaskORM, records=[TaskORM(**self.stats)])

        try:
            self.run()
            self.finish()
        except Exception as e:
            self.fail(e)
        finally:
            self.stats_builder()


class Job:
    STATUS_NOT_STARTED = 'not started'
    STATUS_STARTED = 'started'
    STATUS_FINISHED = 'finished'
    STATUS_FAILED = 'failed'

    def __init__(self, name: str, db_config:dict, cosmos_path:str, app_code:Optional[str]=None):
        self.tasks_stats = None
        self.name = name
        self.db_config = db_config
        self.job_id = uuid.uuid4()
        self.app_code = app_code
        self.tasks = []
        self.start_time = None
        self.end_time = None
        self.status = Job.STATUS_NOT_STARTED
        self.duration = None
        self.job_exception = None
        self.cosmos_path = cosmos_path
        self.stats = {
            'name': self.name,
            'app_code': self.app_code,
            'job_id': self.job_id,
            'memory_usage_start': 0,
            'memory_usage_end': 0,
            'memory_usage':0,
            'cpu_usage_start': 0,
            'cpu_usage_end': 0,
            'cpu_usage':0,
            'started_at': None,
            'ended_at': None,
            'duration': None,
            'status': self.status,
            'exception': None,
            'host_name': socket.gethostname(),
            'execution_user': getpass.getuser(),
            'process_id': os.getpid(),
            'number_of_tasks': 0,
        }

    def add_task(self, task: Task):
        self.tasks.append(task)

    def start(self):
        self.start_time = dt.now()
        self.stats['started_at'] = self.start_time.isoformat() if self.start_time else None
        self.status = Job.STATUS_STARTED

        # Prime CPU measure and reset
        psutil.cpu_percent(interval=1)  # Priming measure (reset state)
        self.stats['memory_usage_start'] = psutil.virtual_memory().used / (1024 ** 2)
        self.stats['cpu_usage_start'] = psutil.cpu_percent(interval=1)

        logging.info(f'Job {self.name}: {self.status}')

    def finish(self):
        self.end_time = dt.now()
        self.stats['ended_at'] = self.end_time.isoformat() if self.end_time else None
        self.status = Job.STATUS_FINISHED

        psutil.cpu_percent(interval=1)  # Priming measure (reset state)
        self.stats['memory_usage_end'] = psutil.virtual_memory().used / (1024 ** 2)
        self.stats['cpu_usage_end'] = psutil.cpu_percent(interval=1)

        logging.info(f'Job {self.name}: {self.status}')

    def fail(self, exception: Optional[Exception] = None):
        self.end_time = dt.now()
        self.stats['ended_at'] = self.end_time.isoformat() if self.end_time else None
        self.status = Job.STATUS_FAILED
        self.stats['exception'] = self.job_exception or str(exception)

        psutil.cpu_percent(interval=1)  # Priming measure (reset state)
        self.stats['memory_usage_end'] = psutil.virtual_memory().used / (1024 ** 2)
        self.stats['cpu_usage_end'] = psutil.cpu_percent(interval=1)

        self.stats_builder()
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
        db = DB(db_config=self.db_config)
        db.records_loader(model=JobORM, records=[JobORM(**self.stats)])

        try:
            for task in self.tasks:
                task.execute(db=db)
            self.finish()
        except Exception as e:
            self.fail(e)
        finally:
            self.stats_builder()

    def add_tasks(self, *tasks):
        raise NotImplementedError('Subclasses must implement this method')

    def run_job(self):
        # Adding Job Tasks
        self.add_tasks()

        # Job Execution
        self.execute()

        # DB Init
        db = DB(db_config=self.db_config)

        job_stats, tasks_stats = self.stats_builder()

        # STATS
        # - Stats Data To Daedalus
        session = None
        try:
            logging.info(f"Sending Job {job_stats.get('job_id')} stats to Daedalus DB")
            #db_session = DBSession(**self.db_config)
            #session = db_session.create()

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
                'started_at': job_stats.get('started_at'),
                'ended_at': job_stats.get('ended_at'),
                'duration': job_stats.get('duration'),
                'memory_usage': job_stats.get('memory_usage'),
                'cpu_usage': job_stats.get('cpu_usage'),
                'host_name': job_stats.get('host_name'),
                'execution_user': job_stats.get('execution_user'),
                'process_id': job_stats.get('process_id'),
                'number_of_tasks': job_stats.get('number_of_tasks'),
                'app_code': job_stats.get('app_code')
            }

            # Loading Stats
            db.records_loader(model=JobORM, records=[JobORM(**job_data)])
            db.records_loader(model=TaskORM, records=[TaskORM(**x) for x in tasks_stats])

            #session.commit()

        except Exception as e:
            #if session:
            #    session.rollback()
            logging.warning(
                f"Session rollback --> Exception occurred: {e}\n"
                f"Traceback details:\n{traceback.format_exc()}"
            )

        finally:
            #session.close()
            db.session.close()

            # - Stats Data To Cosmos
            logging.info(f"Writing Job {job_stats.get('job_id')} stats in {os.path.join(self.cosmos_path, 'stats/')}")

            for record in tasks_stats:
                for key in ['job_id','task_id','pipeline_code','source_code','task_type_code']:
                    record[key] = str(record.get(key))

            for key in ['job_id','app_code']:
                job_stats[key] = str(job_stats.get(key))

            try:
                for stats in [(job_stats, 'J'), (tasks_stats, 'T')]:
                    if len(stats[0])==0:
                        logging.warning(f'No data coming from {stats}')
                    else:
                        BucketHandler(path=self.cosmos_path).exporter(data=stats[0],
                                                                      file_name=f'{stats[1]}_{self.job_id}',
                                                                      folder='jobs/',
                                                                      mode='wt')

            except Exception as e:
                logging.error(f'Exception raised when loading stats into Cosmos --> {str(e)}')


class TaskManager:
    def __init__(self, system_path: str, cosmos_path: str, job_id: str, pipeline_code: str, db_config: dict):
        self.system_path = system_path
        self.cosmos_path = cosmos_path
        self.job_id = job_id
        self.pipeline_code = pipeline_code
        self.db_config = db_config
        self.tasks = []

    def add_task(self):
        e = NotImplementedError('Subclasses must implement this method')
        raise e

    def get_tasks(self):
        return self.tasks

