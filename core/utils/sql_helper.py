
from typing import Optional
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError
import logging


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

    def records_loader(self, model: declarative_base, records: list[dict], commit=True):

        primary_keys = list(model.__table__.primary_key.columns.keys())

        if len(primary_keys) > 1:
            raise ValueError(f"Composite primary keys are not supported: {primary_keys}")

        primary_key = primary_keys[0]

        ### When a record don't have a primary_key key will be treated as a new record (by default the DB will create
        # for each record a UUID)
        logging.info('Getting records to insert')
        insert_records = [record for record in records if primary_key not in record.keys()]

        logging.info('Getting records to update Keys')
        update_records = [record for record in records if primary_key in record.keys()]

        existing_keys = {
            key for (key,) in self.session.query(getattr(model, primary_key))
            .filter(getattr(model, primary_key).in_([record.get(primary_key) for record in update_records]))
        }
        logging.info('Getting records to update')
        update_records = [record for record in update_records if record.get(primary_key) in existing_keys]

        logging.info('Loading data to the DB')
        try:
            if update_records:
                logging.info(f"Records to update: {len(update_records)}")
                self.session.bulk_update_mappings(model, update_records)
            if insert_records:
                logging.info(f"Records to insert: {len(insert_records)}")
                self.session.bulk_insert_mappings(model, insert_records)

            if commit:
                self.session.commit()

            return {"updated": len(update_records), "inserted": len(insert_records), "skipped": len(records) - len(update_records) - len(insert_records)}


        except Exception as e:
            self.session.rollback()
            logging.error(f"Error during bulk load: {e}")
            logging.debug(f"Failed insert records: {insert_records}")
            logging.debug(f"Failed update records: {update_records}")
            raise