import uuid
import logging
import json
from typing import Optional, Union
from sqlalchemy import create_engine, MetaData, select, func
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError
from core.utils.general_helper import json_cleaner
from core.model.elysium.model_data_ops import Source as SourceORM, Task as TaskORM, AuditLog as AuditLogORM, AuditLog


class DBConnection:
    def __init__(self, db_location: str):
        self.location = db_location
        self.engine = None
        self.metadata = None

    def connect(self):
        try:
            # Engine creation
            self.engine = create_engine(self.location)
            logging.info(f"Connection to {self.location.split('/')[-1]}: established")

        except SQLAlchemyError as e:
            logging.exception(f"Connection to {self.location.split('/')[-1]}: failed")
            self.engine = None
            raise

    def get_engine(self):
        if not self.engine:
            self.connect()
        return self.engine

    def get_metadata(self):
        if not self.metadata:
            if not self.engine:
                self.connect()
            # Initialize metadata without binding the engine
            self.metadata = MetaData()
            # Reflect the database schema into the metadata object
            self.metadata.reflect(bind=self.engine)  # Explicitly reflect using the engine
        return self.metadata


class DBSession:
    def __init__(self, db_user: str, db_pass: str, db_host: str, db_port: int, db_name: str):
        self.location = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
        self.connection = DBConnection(self.location)

        try:
            self.engine = self.connection.get_engine()
        except SQLAlchemyError:
            self.engine = None
            logging.error("Engine not initialized due to connection failure")

        if self.engine:
            self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        else:
            self.SessionLocal = None
            logging.error("SessionLocal not initialized")

        self.metadata = self.connection.get_metadata()

    def create(self):
        if self.SessionLocal is None:
            logging.error("SessionLocal not initialized")
            return None

        try:
            session = self.SessionLocal()
            logging.info("Session created")
            return session
        except SQLAlchemyError as e:
            logging.exception("Session creation failed")
            return None

    def close(self, session):
        try:
            if session and session.is_active:
                session.close()
                logging.info("Session closed")
            else:
                logging.warning("Session is not active or is None, nothing to close")
        except SQLAlchemyError as e:
            logging.exception("Session close failed")


class DBInitializer:
    def __init__(self, session: DBSession):
        if session and session.engine and session.metadata:
            self.session = session
        else:
            logging.error("DBInitializer requires a valid session with initialized engine and metadata")
            self.session = None

    def db_init(self, tables):
        if not self.session or not self.session.engine:
            logging.error("Database engine is not initialized. Cannot create tables.")
            return

        for table in tables:
            try:
                if hasattr(table, '__table__'):
                    table.__table__.create(bind=self.session.engine, checkfirst=True)
                    logging.info(f"Table {table.__tablename__} created or already present in the DB")
                else:
                    logging.error(f"Invalid table object: {table}. Table must have a __table__ attribute.")
            except SQLAlchemyError:
                logging.exception(f"Table {table.__tablename__} creation failed")
            except Exception:
                logging.exception(f"Unexpected error during table creation for {table.__tablename__}")


class DB:
    def __init__(self, db_config: dict, orm_objects: Optional[list[declarative_base]] =None ):
        self.dbconfig = db_config
        self.orm_objects = orm_objects

        db_session = DBSession(**db_config)
        self.session = db_session.create()

        self.db_init()

    def db_init(self):
        try:
            logging.info('Initializing DB')
            db_init = DBInitializer(session=DBSession(**self.dbconfig))
            self.session.begin()  # Start transaction

            if self.orm_objects is not None:
                db_init.db_init(self.orm_objects)
                self.session.commit()

        except SQLAlchemyError as e:
            self.session.rollback()
            logging.error(f"Error initializing DB: {e}")
            raise

    def get_session(self):
        return self.Session()

    def records_loader(self, model: declarative_base, records: list[Union[dict,object]], commit=True, task_id:Optional[
        uuid.UUID]=None):

        primary_keys = list(model.__table__.primary_key.columns.keys())

        if len(primary_keys) > 1:
            raise ValueError(f"Composite primary keys are not supported: {primary_keys}")

        primary_key = primary_keys[0]

        # Type of Load
        if records:
            load_type = 'dictionary' if isinstance(records[0], dict) else 'object'
        else:
            load_type = None

        logging.info(f"Loading {load_type} records from {model.__tablename__}")

        # initializing Lineage Metadata
        for record in records:
            if task_id is not None:
                if load_type == 'dictionary':
                    record['lineage_metadata'] = record.get('lineage_metadata', {'modified_in': {'task': str(task_id)}})
                else:
                    record.lineage_metadata = {'modified_in': {'task': str(task_id)}}
            else:
                if load_type == 'dictionary':
                    record['lineage_metadata'] = record.get('lineage_metadata', {})
                else:
                    record.lineage_metadata = {}

        # Listing Records by Type
        if load_type == 'dictionary':
            insert_records = [record for record in records if primary_key not in record.keys()]
            update_records = [record for record in records if primary_key in record.keys()]

            # Records with PK not in DB (When loading DB from old records) -- Missing Records
            existing_keys = {
                key for (key,) in self.session.query(getattr(model, primary_key))
                .filter(getattr(model, primary_key).in_([record.get(primary_key) for record in update_records]))
            }
            missing_records = [record for record in update_records if record.get(primary_key) not in existing_keys]

            # Final update records (records that exist in DB)
            update_records = [record for record in update_records if record.get(primary_key) in existing_keys]

        else:
            insert_records = [record for record in records if getattr(record, primary_key, None) is None]
            insert_records = [record.__dict__.copy() for record in insert_records]

            update_records = [record for record in records if getattr(record, primary_key, None) is not None]

            # Records with PK not in DB (When loading DB from old records) -- Missing Records
            existing_keys = {
                key for (key,) in self.session.query(getattr(model, primary_key))
                .filter(getattr(model, primary_key).in_([getattr(record, primary_key, None) for record in update_records]))
            }
            missing_records = [record for record in update_records if getattr(record, primary_key, None) not in existing_keys]
            missing_records = [record.__dict__.copy() for record in missing_records]

            # Final update records (records that exist in DB)
            update_records = [record for record in update_records if getattr(record, primary_key, None) in existing_keys]
            update_records = [record.__dict__.copy() for record in update_records]
            for record in update_records:
                record.pop("_sa_instance_state", None)

        insert_records.extend(missing_records)

        # Audit Logs Payload
        audit_logs = None
        excluded_schemas = ["operations"]

        if task_id is not None and getattr(model.__table_args__, "schema", None) not in excluded_schemas:
            audit_logs = []
            # INSERTS
            for record in insert_records:
                audit_logs.append(AuditLogORM(table_name=model.__tablename__,
                                              record_id=str(record[primary_key]),
                                              task_id=task_id,
                                              operation_type_code='C',
                                              previous_value=None,
                                              new_value=json.dumps(json_cleaner(record)))
                                  )

            # UPDATES
            if len(update_records)>0:
                # Querying Existing Records
                conditions = getattr(model.__table__.c, primary_key).in_(existing_keys)
                query = select(*model.__table__.columns).where(conditions)
                existing_records = self.session.execute(query).fetchall()

                existing_dict = {str(getattr(record, primary_key)): dict(record._mapping) for record in existing_records}

                for record in update_records:
                    record_id = str(record[primary_key])
                    previous_value = existing_dict.get(record_id, {})

                    # Keeping only changed values in both, previous and current (changes)
                    changes = {k: v for k, v in record.items() if previous_value.get(k) != v}
                    previous_value = {k: previous_value[k] for k in changes}

                    if changes:  # Only log if there are actual changes
                        audit_logs.append(AuditLogORM(
                            table_name=model.__tablename__,
                            record_id=record_id,
                            task_id=task_id,
                            operation_type_code='U',
                            previous_value=json.dumps(json_cleaner(previous_value)),
                            new_value=json.dumps(json_cleaner(changes))
                        ))

        # Loading Data to DB
        logging.info('Loading data to the DB')
        try:
            if update_records:
                logging.info(f"Records to update: {len(update_records)}")
                self.session.bulk_update_mappings(model, update_records)

            if insert_records:
                logging.info(f"Records to insert: {len(insert_records)}")
                self.session.bulk_insert_mappings(model, insert_records)

            if audit_logs is not None:
                logging.info(f"Audit logs to insert: {len(audit_logs)}")
                self.session.bulk_save_objects(audit_logs)

            if commit:
                self.session.commit()

            return {"updated": len(update_records),
                    "inserted": len(insert_records),
                    "skipped": len(records) - len(update_records) - len(insert_records),
                    "logs": len(audit_logs) if audit_logs is not None else 0}

        except Exception as e:
            self.session.rollback()
            logging.error(f"Error during bulk load: {e}")
            logging.debug(f"Failed insert records: {insert_records}")
            logging.debug(f"Failed update records: {update_records}")
            raise


class ConfigSources:
    def __init__(self, db_config:dict):
        self.db_config = db_config
        self.config_data = None

        # Session
        self.session = DBSession(**self.db_config).create()

    def close_session(self):
        self.session.close()

    def config(self, pipeline_code:str, close_session:bool=True) -> dict:
        # JOB Config and Init
        # - Reading Configuration File
        query = select(*SourceORM.__table__.columns).where((SourceORM.pipeline_code == pipeline_code) &
                                                           (SourceORM.active == True))
        config_data = self.session.execute(query).mappings().all()

        if close_session:
            self.close_session()

        return config_data

    def source_latest_image(self,
                            source_code:str,
                            task_type_code:str,
                            status:str,
                            close_session:bool=True) -> str:

        # Conditionals
        conditionals = ((TaskORM.task_type_code == task_type_code) &
                        (TaskORM.source_code == source_code) &
                        (TaskORM.status == status))

        # SubQueries
        subquery = select(func.max(TaskORM.ended_at)).where(conditionals).scalar_subquery()

        # Execution
        query = select(TaskORM.task_image).where(conditionals & (TaskORM.ended_at == subquery))
        path = self.session.execute(query).mappings().all()[0].get('task_image')

        if close_session:
            self.close_session()

        return path