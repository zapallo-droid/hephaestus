from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
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