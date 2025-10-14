# --General Libraries--
import os
import uuid
import logging
import hashlib
import json
from datetime import datetime, timezone
from typing import Optional
from dotenv import load_dotenv
from git import Repo, InvalidGitRepositoryError, NoSuchPathError
from sqlalchemy.orm import Session

# --Project Resources--
from hephaestus_forge.core.model.task import Task
from hephaestus_forge.core.orm.data_ops_model import JobORM, StatusEnum as Status
from hephaestus_forge.core.orm.data_ops_helpers import default_stats
from hephaestus_forge.core.utils.resource_monitor import environment_metadata_collector, resource_monitor

load_dotenv()

# --Core Classes--
class Job:

    def __init__(self, app_code:str, name: str, tasks: list[Task]):

        app_version = self.app_hash_generator(app_code)

        self.tasks = tasks
        self.session = None
        self.job_id = uuid.uuid4()

        self.orm = JobORM(job_id=self.job_id,
                          job_name=name,
                          app_code=app_code,
                          app_version =app_version,
                          app_hash = app_version.get('hash'),
                          job_hash = self.job_hash_generator(name, app_version),
                          job_stats=default_stats(),
                          environment_metadata=environment_metadata_collector(),
                          number_of_tasks=len(tasks) if tasks else 0,
                          status=Status.PENDING,
                          exception=None,
                          job_digest=None)


    @staticmethod
    def app_hash_generator(app_code: str, repo_path=".") -> dict:
        """
        Generates a hash based on Git metadata and app code.

        Parameters:
            app_code (str): Identifier or code of the app.
            repo_path (str): Path to the Git repository (default: current directory).

        Returns:
            dict: Dictionary containing Git metadata, the app code, and a unique hash.
        """
        try:
            repo = Repo(repo_path, search_parent_directories=True)
            commit = repo.head.commit.hexsha
            branch = repo.active_branch.name if not repo.head.is_detached else "detached"
            remote_url = next(repo.remote().urls, "unknown")
        except (InvalidGitRepositoryError, NoSuchPathError, ValueError):
            commit = branch = remote_url = "unknown"

        content = {"git": {"commit": commit, "branch": branch, "repo": remote_url}, "code": app_code}
        serialized = json.dumps(content, sort_keys=True)
        content["hash"] = hashlib.md5(serialized.encode()).hexdigest()
        return content

    @staticmethod
    def job_hash_generator(job_name: str, app_version: dict) -> str:
        content = f"{job_name}_{app_version.get('hash')}"
        return hashlib.md5(content.encode()).hexdigest()

    def digest_generator(self) -> str:
        stats = self.orm.job_stats or {}
        env = self.orm.environment_metadata or {}
        return f"""## JOB {self.orm.job_id} — {self.orm.job_name}
**Status**: {self.orm.status.value}
**App**: {self.orm.app_code}
**Started at**: {env.get('started_at')}
**Ended at**: {env.get('ended_at')}
**Duration**: {stats.get('duration', 'N/A')} seconds
**User**: {env.get('execution_user')}
**Host**: {env.get('host_name')}
**PID**: {env.get('process_id')}
**CPU Usage**: {stats.get('cpu_usage', 'N/A')}%
**Memory Usage**: {stats.get('memory_usage', 'N/A')} MB
**Tasks Executed**: {self.orm.number_of_tasks}
**Warnings**: {len(stats.get('warnings', []))}
**Errors**: {len(stats.get('errors', []))}
**Exception**: {self.orm.exception or 'None'}
"""

    def start(self):
        self.orm.status = Status.STARTED
        self.session.commit()
        logging.info(f"Job {self.orm.job_name}: {self.orm.status}")

    def finish(self, status: Status, exception: Optional[Exception] = None):
        self.orm.status = status
        if status == Status.FAILED:
            self.orm.exception = str(exception) if exception else self.orm.exception
            logging.info(f"Job {self.orm.job_name}: {self.orm.status}. Exception: {self.orm.exception}")
        else:
            logging.info(f"Job {self.orm.job_name}: {self.orm.status}")


    def run(self, task_args:Optional[dict]=None):

        logging.info(f'Job {self.orm.job_name}: {self.orm.job_hash} - Initialized')

        self.start()

        logging.info('Executing Tasks in Session')
        run_timestamp = datetime.now(timezone.utc)
        for task in self.tasks or []:
            task.execute(
                job_id=self.orm.job_id,
                session=self.session,
                task_args=task_args or {},
                run_timestamp=run_timestamp,
            )


    def execute(self, session, task_args: Optional[dict] = None):

        self.session = session
        self.session.add(self.orm)
        self.session.commit()

        try:
            logging.info(f"Starting resource-monitored execution for job: {self.orm.job_name}")

            results, stats = resource_monitor(lambda: self.run(task_args))()

            self.orm.job_stats = {**(self.orm.job_stats or {}), **stats}
            self.orm.environment_metadata["ended_at"] = self.orm.job_stats.get("ended_at")

            self.finish(status=Status.FINISHED)

            self.orm.job_digest = self.digest_generator()

            self.session.flush()
            self.session.commit()
            logging.info(f"Job {self.orm.job_name}: Execution finished.")


        except Exception as e:
            stats = getattr(e, "resource_monitor_stats", None)

            if stats:
                merged_stats = {**(self.orm.job_stats or {}), **stats}
                self.orm.job_stats = merged_stats
                ended_at = merged_stats.get("ended_at") or stats.get("ended_at")
                if ended_at:
                    self.orm.environment_metadata["ended_at"] = ended_at

            self.finish(status=Status.FAILED, exception=e)

            self.orm.job_stats["exception"] = True
            self.orm.job_stats["exception_msg"] = str(e)
            errors = self.orm.job_stats.setdefault("errors", [])
            if str(e) not in errors:
                errors.append(str(e))

            if not self.orm.environment_metadata.get("ended_at"):
                self.orm.environment_metadata["ended_at"] = datetime.now(timezone.utc).isoformat()

            self.orm.job_digest = self.digest_generator()

            try:
                self.session.flush()
                self.session.commit()

            except Exception:
                self.session.rollback()

            raise

