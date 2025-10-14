# --General Libraries--
import os
import logging
import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager
from typing import Generator, Optional, Mapping, Union


# --Script Resources--
load_dotenv()

# --Core Classes--
class SQLConnector:
    """
    """

    def __init__(self, db_config: Optional[Union[Mapping[str, str], str]] = None, echo: Optional[bool] = False):

        env = os.getenv('ENV')
        if not env:
            raise ValueError("Environment variable 'ENV' is not set.")

        self.env: str = env.lower()
        if not self.env:
            raise ValueError("Environment variable 'ENV' is not set.")

        if isinstance(db_config, dict):
            self.db_config: dict = db_config
        elif isinstance(db_config, str):
            self.db_config = self.get_db_config(prefix=db_config.upper())
        else:
            raise TypeError("db_config must be of type str or dict")

        self.location: str = self.get_location()
        self.engine = self.db_connection(echo=echo)
        self.SessionLocal = sessionmaker(bind=self.engine, autoflush=False, future=True)

    def get_db_config(self, prefix:str) -> dict:
        """Loads DB config from environment variables."""
        logging.info(f"Getting (*{prefix}*) database configuration")

        db_config = {
            "db_env": self.env,
            "db_host": os.getenv(f"{prefix}_DB_HOST"),
            "db_port": os.getenv(f"{prefix}_DB_PORT"),
            "db_user": os.getenv("USER_ID"),
            "db_pass": os.getenv("USER_PASS"),
            "db_name": f"{prefix.lower()}_{self.env}"
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

    @contextmanager
    def session(self) -> Generator[Session, None, None]:
        """Yields a database session with auto commit-rollback-close."""
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

