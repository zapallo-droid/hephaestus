import os
import logging
from dotenv import load_dotenv
from hephaestus_pframe.core.utils.sql_helper import DB
from hephaestus_pframe.core.model.elysium.model_data_ops import (App, PipelineDomain, Pipeline, Source, Job,
                                                                 TaskType, Task, OperationType, AuditLog, TaskTypeEnum,
                                                                 OperationsTypeEnum)

load_dotenv()

def get_elysium_config(env: str = "DEV"):
    return {
        'db_user': os.getenv('DB_USER'),
        'db_pass': os.getenv('DB_PASS'),
        'db_host': os.getenv(f'{env.upper()}_HOST'),
        'db_port': os.getenv(f'{env.upper()}_PORT'),
        'db_name': f'elysium_{env.lower()}'
    }

def init_operations_schema(env: list[str]=None):

    if env is None:
        env = ['DEV','PROD']

    for e in env:
        db_config = get_elysium_config(e)

        orm_objects = [App, PipelineDomain, Pipeline, Source, Job, TaskType, Task,
                       OperationType, AuditLog]

        db = DB(db_config=db_config, orm_objects=orm_objects)
        db.db_init()

        logging.info(f"Schema 'operations' initialized in environment: {e}")
        logging.info(f"Inserting in 'operations' the standard types for Tasks and Operations:")

        # Standard inserts
        records = [{'task_type_code': record.code, 'task_type_name': record.label} for record in TaskTypeEnum]
        db.records_loader(model=TaskType, records=records)

        records = [{'operation_type_code': record.code, 'operation_type_name': record.label} for record in OperationsTypeEnum]
        db.records_loader(model=OperationType, records=records)

        logging.info(f"Standard values inserted into operations.task_type and operations.operation_type of env: {e}")

if __name__ == "__main__":
    init_operations_schema()