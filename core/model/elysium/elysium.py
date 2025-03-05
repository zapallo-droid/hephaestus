# General libraries for DB
import os
import logging
import gzip
import json
from dotenv import load_dotenv
from sqlalchemy.ext.declarative import declarative_base
from core.utils.general_helper import deterministic_code
from core.model.jobs import Task
from core.utils.sql_helper import DB
from core.model.elysium.resources.role import Role as RoleORM
from core.model.elysium.resources.geopolitical_entities import (GeopoliticalEntity as GeopoliticalEntityORM,
                                                                GeopoliticalSubEntity as GeopoliticalSubEntityORM)
from core.model.elysium.model_data_ops import (App as AppORM, Pipeline as PipelineORM, Job as JobORM,
                                               Task as TaskORM, Source as SourceORM, TaskType as TaskTypeORM,
                                               PipelineDomain as PipelineDomainORM)


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


if __name__ == '__main__':
    # Config and Parameters
    load_dotenv()
    c_path = os.getenv('COSMOS_PATH')
    s_path = os.getcwd()

    app_objects = [AppORM(x) for x in ['hephaestus']]

    pipeline_domains_objects = [PipelineDomainORM(x) for x in ['Parliamentary - Argentina',
                                                               'International Labour Organization',
                                                               'Geopolitical Entities',
                                                               'Argentinian Census']]

    pipeline_objects = [PipelineORM(**x) for x in [{'app_code':deterministic_code(text='hephaestus', max_length=8),
                                                 'pipeline_domain_code':deterministic_code(text='International Labour Organization', max_length=8),
                                                 'pipeline_name':'ILOSTAT-ISCO'},
                                                {'app_code': deterministic_code(text='hephaestus', max_length=8),
                                                 'pipeline_domain_code': deterministic_code(
                                                     text='Parliamentary - Argentina', max_length=8),
                                                 'pipeline_name': 'Argentinian Upper Chamber'},
                                                {'app_code': deterministic_code(text='hephaestus', max_length=8),
                                                 'pipeline_domain_code': deterministic_code(
                                                     text='Parliamentary - Argentina', max_length=8),
                                                 'pipeline_name': 'Argentinian Lower Chamber'},
                                                {'app_code': deterministic_code(text='hephaestus', max_length=8),
                                                 'pipeline_domain_code': deterministic_code(
                                                     text='Parliamentary - Argentina', max_length=8),
                                                 'pipeline_name': 'Argentinian Legislation'},
                                                {'app_code': deterministic_code(text='hephaestus', max_length=8),
                                                 'pipeline_domain_code': deterministic_code(
                                                     text='Geopolitical Entities', max_length=8),
                                                 'pipeline_name': 'ISO 3166-1'},
                                                {'app_code': deterministic_code(text='hephaestus', max_length=8),
                                                 'pipeline_domain_code': deterministic_code(
                                                     text='Geopolitical Entities', max_length=8),
                                                 'pipeline_name': 'ISO 3166-2'},
                                                {'app_code': deterministic_code(text='hephaestus', max_length=8),
                                                 'pipeline_domain_code': deterministic_code(
                                                     text='Argentinian Census', max_length=8),
                                                 'pipeline_name': 'ARG Population'}
                                                ]]

    # for host in ['DEV','PROD']:
    for host in ['DEV']:
        elysium_config = {'db_user': os.getenv('DB_USER'),
                          'db_pass': os.getenv('DB_PASS'),
                          'db_host': os.getenv(f'{host.upper()}_HOST'),
                          'db_port': os.getenv(f'{host.upper()}_PORT'),
                          'db_name': f'elysium_{host.lower()}'}

        # DB Init
        db = DB(db_config=elysium_config,
                orm_objects=[AppORM, PipelineDomainORM, PipelineORM, SourceORM, JobORM, TaskTypeORM, TaskORM,
                             RoleORM, GeopoliticalEntityORM, GeopoliticalSubEntityORM])

        db.db_init()

        db.records_loader(model=AppORM, records=app_objects)

        db.records_loader(model=TaskTypeORM, records=[{'task_type_code': 'E', 'task_type_name': 'extract'},
                                                   {'task_type_code': 'T', 'task_type_name': 'transform'},
                                                   {'task_type_code': 'L', 'task_type_name': 'load'}])

        db.records_loader(model=PipelineDomainORM, records=pipeline_domains_objects)

        db.records_loader(model=PipelineORM, records=pipeline_objects)


        #import json
        #with open('',
        #          'r',
        #          encoding="utf-8") as f:

        #    data=json.load(f)

        #db.records_loader(model=SourceORM,
        #                  records=[SourceORM(**x) for x in data])