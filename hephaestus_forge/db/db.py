# --General Libraries--
import os
import logging
import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager
from typing import Generator, Optional

# --Project Resources--
from hephaestus_forge.core.utils.general_helper import deterministic_code, json_cleaner
from hephaestus_forge.core.orm.base import Base
from hephaestus_forge.core.orm.data_ops_model import (JobORM, TaskORM, AppORM, PipelineORM, PipelineDomainORM,
                                                      SourceORM, AuditLogORM, TransferLogORM)

# --Script Resources--
load_dotenv()
SCHEMA = os.getenv('HEPHAESTUS_SCHEMA', 'data_ops')
ENV = os.getenv('ENV')

# --Core Classes--
class DB:
    """
    """

    def __init__(self, create_all: Optional[bool] = False, echo: Optional[bool] = False):
        env = os.getenv('ENV')
        if not env:
            raise ValueError("Environment variable 'ENV' is not set.")
        self.env = env.lower()
        if not self.env:
            raise ValueError("Environment variable 'ENV' is not set.")

        self.db_config: dict = self.get_db_config()
        self.location: str = self.get_location()
        self.engine = self.db_connection(echo=echo)
        self.SessionLocal = sessionmaker(bind=self.engine, autoflush=False, future=True)
        if create_all:
            self.init_schema()

    def get_db_config(self) -> dict:
        """Loads DB config from environment variables."""
        logging.info("Getting **DataOps** (*Hephaestus*) database configuration")

        db_config = {
            "db_env": self.env,
            "db_host": os.getenv("HEPHAESTUS_DB_HOST"),
            "db_port": os.getenv("HEPHAESTUS_DB_PORT"),
            "db_user": os.getenv("USER_ID"),
            "db_pass": os.getenv("USER_PASS"),
            "db_name": f"hephaestus_{self.env}"
        }

        for key, value in db_config.items():
            if not value:
                raise ValueError(f"Database configuration '{key}' is None")

        return db_config

    def get_location(self) -> str:

        location = (f"postgresql://{self.db_config['db_user']}:{self.db_config['db_pass']}@"
                    f"{self.db_config['db_host']}:{self.db_config['db_port']}/{self.db_config['db_name']}")

        return location

    def db_connection(self, echo: Optional[bool] = False):
        """Creates and return the Engine"""
        try:
            engine = create_engine(self.location, echo=echo)
            logging.info(f"Connection to {self.location.split('/')[-1]}: established")
        except SQLAlchemyError as e:
            logging.exception(f"Connection to {self.location.split('/')[-1]}: failed")
            engine = None
        return engine

    def init_schema(self):
        """Initialize the Schema (Creates all the Tables)"""
        try:
            logging.info(f"Initializing schema in Env: {self.env}")
            Base.metadata.create_all(self.engine)
            logging.info(f"Schema in {self.env} initialized")
        except Exception as e:
            logging.exception(f"Initializing schema in Env: {self.env} failed")
            raise

    def drop_schema(self):
        """Drop the Schema (Drop all the Tables)"""
        try:
            logging.info(f"Dropping schema in Env: {self.env}")
            Base.metadata.drop_all(self.engine)
            logging.info(f"Schema in {self.env} dropped")
        except Exception as e:
            logging.exception(f"Dropping schema in Env: {self.env} failed")
            raise

    def reset_schema(self):
        """Drop and Initialize the Schema (Drop first and then Creates all the Tables)"""
        self.drop_schema()
        self.init_schema()

    @contextmanager
    def session(self) -> Generator[Session, None, None]:
        """Yields a database session with auto commit/rollback/close."""
        session = self.SessionLocal()
        try:
            logging.info(f"Yielding session in Env: {self.env}")
            yield session
            session.commit()
        except Exception as e:
            logging.exception(f"Yielding session in Env: {self.env} failed: {e}")
            session.rollback()
            raise e
        finally:
            logging.info(f"Closing session in Env: {self.env}")
            session.close()
            logging.info(f"Session in Env: {self.env} closed")


class ProjectConfigDB:
    def __init__(self, config_path: Optional[str] = "config/project_config.yaml"):
        self.config_path = config_path
        self.data = {}

    def read_config(self):
        with open(self.config_path, "r", encoding='utf-8') as f:
            self.data = yaml.load(f, Loader=yaml.FullLoader)

    def write_config(self):
        with open(self.config_path, "w", encoding='utf-8') as f:
            yaml.safe_dump(self.data, f, sort_keys=False, allow_unicode=True)

    def load_config(self, session: Session):
        try:
            self.read_config()

            # -- APP --
            app_payload = self.data.get("app")
            app_code = deterministic_code(app_payload.get("app_name"))
            app_payload["app_code"] = app_code

            app = AppORM(**json_cleaner(app_payload))
            session.merge(app)

            # -- DOMAINS / PIPELINES / SOURCES --
            for domain in self.data.get("pipeline_domains", []):
                domain_code = deterministic_code(domain.get("pipeline_domain_name"))
                domain["pipeline_domain_code"] = domain_code

                domain_payload = {
                    "pipeline_domain_name": domain.get("pipeline_domain_name"),
                    "pipeline_domain_code": domain_code,
                }
                session.merge(PipelineDomainORM(**json_cleaner(domain_payload)))

                for pipeline in domain.get("pipelines", []):
                    pipeline_code = deterministic_code(pipeline.get("pipeline_name"))
                    pipeline["pipeline_code"] = pipeline_code
                    pipeline["pipeline_domain_code"] = domain_code
                    pipeline["app_code"] = app_code

                    pipeline_payload = {
                        "pipeline_name": pipeline.get("pipeline_name"),
                        "pipeline_code": pipeline_code,
                        "pipeline_domain_code": domain_code,
                        "app_code": app_code,
                    }
                    session.merge(PipelineORM(**json_cleaner(pipeline_payload)))

                    for source in pipeline.get("sources", []):
                        source_code = deterministic_code(source.get("source_name"))
                        source["source_code"] = source_code
                        source["pipeline_code"] = pipeline_code

                        source_payload = {
                            "source_code": source_code,
                            "source_name": source.get("source_name"),
                            "location_type": source.get("location_type"),
                            "location": source.get("location"),
                            "location_endpoint": source.get("location_endpoint"),
                            "extension": source.get("extension"),
                            "extract_type": source.get("extract_type"),
                            "params": source.get("params"),
                            "headers": source.get("headers"),
                            "timeout": source.get("timeout"),
                            "pipeline_code": pipeline_code,
                            "active": source.get("active", True),
                        }
                        session.merge(SourceORM(**json_cleaner(source_payload)))

            session.commit()
            self.write_config()
            logging.info("Project initialized and project_config.yaml updated")

        except Exception as e:
            logging.exception(f"Project Initialization Failed: {e}")
            session.rollback()
            raise e


if __name__ == '__main__':
    logging.info(f"Starting DB in SCHEMA: {SCHEMA} and ENV: {ENV}")
    DB(create_all=True)


