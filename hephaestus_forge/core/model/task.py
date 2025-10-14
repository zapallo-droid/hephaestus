# --General Libraries--
import uuid
import logging
from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert

# --Project Resources--
from hephaestus_forge.core.orm.data_ops_model import (TaskORM, StatusEnum, TaskTypeEnum, AuditLogORM, TransferLogORM,
                                                      OperationsTypeEnum)
from hephaestus_forge.core.utils.resource_monitor import resource_monitor
from hephaestus_forge.core.orm.data_ops_helpers import default_stats
from hephaestus_forge.core.orm.base import Base
from hephaestus_forge.core.utils.general_helper import json_cleaner


# --Core Classes--
class Task:

    def __init__(self, task_name: str, pipeline_code: str, task_type: TaskTypeEnum):
        self.orm: Optional[TaskORM] = None
        self.task_id = uuid.uuid4()
        self.task_name = task_name
        self.pipeline_code = pipeline_code
        self.task_type = task_type

    def digest_generator(self) -> str:

        stats = (self.orm.task_stats if self.orm else {}) or {}
        return f"""## Task Digest: `{self.orm.task_name}`
**Task ID**: `{self.orm.task_id}`
**Status**: `{self.orm.status.value}`
**Job ID**: `{self.orm.job_id}`
**Pipeline**: `{self.orm.pipeline_code}`
**Type**: `{self.orm.task_type.value}`

### Execution
- **Start**: `{stats.get('started_at')}`
- **End**: `{stats.get('ended_at')}`
- **Duration**: `{stats.get('duration')}s`

### Resources
- **CPU**: `{stats.get('cpu_usage')}%`
- **Memory**: `{stats.get('memory_usage')} MB`

### Records
- **Input**: `{stats.get('record_count_input')}`
- **Output**: `{stats.get('record_count_output')}`
- **Skipped**: `{stats.get('record_count_skipped')}`

### Issues
- **Warnings**: `{len(stats.get('warnings', []))}`
- **Errors**: `{len(stats.get('errors', []))}`
{"### Exception\n```text\n" + str(self.orm.exception) + "\n```" if self.orm and self.orm.status == StatusEnum.FAILED and self.orm.exception else ""}
"""

    ## --LOGS-- ##
    def log_transfer(self, session, *, source_code, input_path, output_path, input_format, output_format, transfer_type):
        log_payload = TransferLogORM(log_id=uuid.uuid4(),
                                     task_id=self.orm.task_id,
                                     source_code=source_code,
                                     input_path=input_path,
                                     output_path=output_path,
                                     input_format=input_format,
                                     output_format=output_format,
                                     transfer_type=transfer_type)

        session.add(log_payload)

    def log_audit(self, session, *, table_name, record_id, operation, previous, new):
        log_payload = AuditLogORM(log_id=uuid.uuid4(),
                                  task_id=self.orm.task_id,
                                  table_name=table_name,
                                  record_id=record_id,
                                  operation_type=operation,
                                  previous_value=previous,
                                  new_value=new)

        session.add(log_payload)

    def upsert_logged_records(self, orm_class, records, pk_name:Optional[str]= 'id',
                              business_session:Optional[Session]=None,
                              cols_to_exclude_in_update:Optional[list]=None) -> None:

        if business_session is None:
            business_session = self.session

        table = orm_class.__table__
        table_name = orm_class.__tablename__

        if not records:
            logging.info({"existing": 0, "inserted": 0, "updated": 0})
            return

        ids = [r[pk_name] for r in records if pk_name in r]

        ##-- Existing
        if ids:
            stmt = select(table).where(getattr(orm_class, pk_name).in_(ids))
            existing = business_session.execute(stmt).mappings().all()
        else:
            existing = []

        existing_set = {record[pk_name]: dict(record) for record in existing}
        existing_pks = set(existing_set.keys())

        ##-- Upserting in Business DB
        insert_stmt = pg_insert(table).values(records)

        updatable_cols = [c for c in table.c if not c.primary_key]

        if cols_to_exclude_in_update and isinstance(cols_to_exclude_in_update, list):
            excl = set(cols_to_exclude_in_update)
            updatable_cols = [c for c in updatable_cols if c.name not in excl]

        update_set = {c.name: getattr(insert_stmt.excluded, c.name) for c in updatable_cols}

        upsert_stm = insert_stmt.on_conflict_do_update(index_elements=[table.c[pk_name]], set_=update_set)

        try:
            business_session.execute(upsert_stm)
            business_session.flush()

            inserted = 0
            updated = 0

            for record in records:
                record_id = record[pk_name]

                if record_id in existing_pks:
                    updated += 1

                    self.log_audit(self.session,
                                   table_name=table_name,
                                   record_id=record_id,
                                   operation=OperationsTypeEnum.UPDATE,
                                   previous=json_cleaner(existing_set.get(record_id, {})),
                                   new=json_cleaner(record))

                else:
                    inserted += 1
                    self.log_audit(self.session,
                                   table_name=table_name,
                                   record_id=record_id,
                                   operation=OperationsTypeEnum.CREATE,
                                   previous=json_cleaner({}),
                                   new=json_cleaner(record))

            business_session.commit()
            #self.session.commit() ###
            logging.info({"existing": len(existing), "inserted": inserted, "updated": updated})

        except Exception as e:
            business_session.rollback()
            raise

    ## --PIPELINE-- ##
    def start(self):
        self.orm.status = StatusEnum.STARTED
        self.session.commit()
        logging.info(f'Job {self.orm.task_name}: {self.orm.status}')

    def finish(self, status: StatusEnum, exception: Optional[Exception] = None):
        self.orm.status = status

        if status == StatusEnum.FAILED:
            self.orm.exception = str(exception)
            logging.info(f'''Job {self.orm.task_name}: {self.orm.status}. --> 
                                     Exception: {str(exception)}''')
        else:
            logging.info(f'Job {self.orm.task_name}: {self.orm.status}')


    def run(self, session: Session, run_timestamp: datetime, **task_args):
        """
        Actual task logic should be implemented in subclasses.
        """
        raise NotImplementedError("Subclasses must implement the run() method.")

    def execute(self, job_id, run_timestamp:datetime, session: Session, task_args):
        """
        Executes the task with resource monitoring, updates stats in-place.
        """

        self.session = session

        self.orm = TaskORM(task_id=self.task_id,
                           task_name=self.task_name,
                           job_id=job_id,
                           pipeline_code=self.pipeline_code,
                           task_type=self.task_type,
                           status=StatusEnum.PENDING,
                           task_stats=default_stats(),
                           task_digest=None,
                           records_processed=0,
                           files_processed=0)
        session.add(self.orm)
        session.commit()

        ##-- Pipeline
        try:
            logging.info(f"Task {self.orm.task_name}: Starting resource-monitored execution.")
            self.start()

            result, stats = resource_monitor(lambda: self.run(session=session,
                                                              run_timestamp=run_timestamp,
                                                              **task_args))()            

            self.finish(status=StatusEnum.FINISHED)
            self.orm.task_stats.update(stats)

        except Exception as e:
            self.finish(status=StatusEnum.FAILED, exception=e)

            stats = getattr(e, "resource_monitor_stats", None)
            if stats:
                self.orm.task_stats.update(stats)

            self.orm.task_stats["exception"] = True
            self.orm.task_stats["exception_msg"] = str(e)
            errors = self.orm.task_stats.setdefault("errors", [])
            if str(e) not in errors:
                errors.append(str(e))

            self.orm.task_digest = self.digest_generator()
            self.session.flush()
            self.session.commit()

            raise

        self.orm.task_digest = self.digest_generator()
        session.commit()
        logging.info(f"Task {self.orm.task_name}: Execution finished.")
