# General libraries for DB
import logging
import gzip
import json
from sqlalchemy.ext.declarative import declarative_base
from hephaestus.core.model.jobs import Task
from hephaestus.core.utils.sql_helper import DB


class ElysiumLoad(Task):
    def __init__(self, job_id:str, name:str, config:dict, location:str, db_config:dict, model:declarative_base):

        super().__init__(job_id=job_id,
                         name=name,
                         pipeline_code=config.get("pipeline_code"),
                         source_code=config.get("source_code"),
                         location=location,
                         task_type_code='L')

        self.db_config = db_config
        self.model = model
        self.data = None

    def load(self, data:list[dict]):
        schema = getattr(self.model.__table__, "schema", "public")
        self.task_image = f"{schema}.{self.model.__tablename__}"

        if not data:
            e = ValueError(f"No data was provided for loading into {self.task_image}")
            self.fail(e)
            return

        try:
            elysium=DB(db_config=self.db_config)

            elysium.records_loader(model=self.model, records=data, task_id=self.task_id)
            self.records_processed = len(data)
            logging.info(f"{self.records_processed} records successfully loaded into {self.task_image}.")

        except Exception as e:
            self.fail(e)


    def run(self):

        try:
            with gzip.open(self.location, "rt", encoding="utf-8") as f:
                self.data = json.load(f)  # Read and parse JSON data

            self.load(self.data)

        except Exception as e:
            self.fail(e)
