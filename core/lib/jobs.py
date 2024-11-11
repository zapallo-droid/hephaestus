from datetime import datetime as dt
import logging
import psutil
import socket
import getpass
import os
import uuid
from typing import Optional


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
        self.stats['memory_usage'] = self.stats['memory_usage_end'] - self.stats['memory_usage_start']
        self.stats['cpu_usage'] = max(0, self.stats['cpu_usage_end'] - self.stats['cpu_usage_start'])

        return self.stats


class Job:
    def __init__(self, name: str, app_name:Optional[str]=None, app_code:Optional[str]=None,
                 pipeline_code:Optional[str]=None, pipeline_name:Optional[str]=None):
        self.name = name
        self.job_id = f'J-{str(uuid.uuid4())}'
        self.app_name = app_name
        self.app_code = app_code
        self.pipeline_code = pipeline_code
        self.pipeline_name = pipeline_name
        self.tasks = []
        self.start_time = None
        self.end_time = None
        self.status = 'not started'
        self.duration = None
        self.job_exception = None
        self.stats = {}

    def add_task(self, task: Task):
        self.tasks.append(task)

    def start(self):
        self.start_time = dt.now()
        self.status = 'started'

        # Prime CPU measure and reset
        psutil.cpu_percent(interval=1)  # Priming measure (reset state)
        self.stats['name'] = self.name
        self.stats['app_code'] = self.app_code
        self.stats['app_name'] = self.app_name
        self.stats['pipeline_code'] = self.pipeline_code
        self.stats['pipeline_name'] = self.pipeline_name
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
        tasks_stats = [task.stats_builder() for task in self.tasks]

        self.duration = (self.end_time - self.start_time).total_seconds() if self.end_time and self.start_time else None
        self.stats['status'] = self.status
        self.stats['exception'] = str(self.job_exception)
        self.stats['duration'] = self.duration
        self.stats['memory_usage'] = self.stats['memory_usage_end'] - self.stats['memory_usage_start']
        self.stats['cpu_usage'] = max(0, self.stats['cpu_usage_end'] - self.stats['cpu_usage_start'])  # Avoid negative CPU usage
        self.stats['host_name'] = socket.gethostname()
        self.stats['execution_user'] = getpass.getuser()
        self.stats['process_id'] = os.getpid()
        self.stats['number_of_tasks'] = len(self.tasks)
        self.stats['tasks'] = tasks_stats

        return self.stats

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





