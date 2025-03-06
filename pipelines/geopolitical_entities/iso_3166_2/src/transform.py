import os
import json
import gzip
import logging
from tqdm import tqdm
from core.model.jobs import Task
from core.utils.handler_bucket import BucketHandler
from core.utils.sql_helper import ConfigSources
from core.model.elysium.resources.geopolitical_entities import GeopoliticalSubEntity


class GeopoliticalSubEntitiesTransform(Task):
    def __init__(self, job_id:str, bucket_path:str, name:str, config:dict, db_config:dict) -> None:

        super().__init__(job_id=job_id,
                         name=name,
                         pipeline_code=config.get("pipeline_code"),
                         source_code=config.get("source_code"),
                         task_type_code='T')

        self.bucket_path = bucket_path
        self.db_config = db_config
        self.config = config
        self.data = None

    def get_data(self)->list[dict]:
        task_type_code = "E"
        status = "finished"

        try:
            # Getting latest Extract Path (Image) of the given source
            sources_config = ConfigSources(db_config=self.db_config)
            path = sources_config.source_latest_image(source_code=self.source_code,
                                                      task_type_code=task_type_code,
                                                      status=status)

            # Getting raw data
            with gzip.open(path) as f:
                data_temp = json.load(f)
            data = data_temp.get('provincias')

            # Transforming data
            model_col_names = [col.name for col in GeopoliticalSubEntity.__table__.columns]

            for record in tqdm(data, desc='Processing Geopolitical Sub Entities'):
                record['ge_iso_code'] = 'ARG'
                record['gse_iso_code'] = record.pop('iso_id')
                record['name'] = record.pop('iso_nombre')
                record['category'] = 'district' if 'provincia' not in record.get('categoria') else 'state'
                record['geo_point_2d'] = record.pop('centroide')

                keys_to_drop = [col for col in record.keys() if col not in model_col_names]
                for key in keys_to_drop:
                    del record[key]

            return data

        except Exception as e:
            self.fail(e)

    def run(self):
        logging.info(f'Processing file: {self.name}')

        try:
            self.data = self.get_data()

            self.bucket_imaging(data=self.data,
                                pipeline_config=self.config,
                                bucket_path=self.bucket_path)

        except Exception as e:
            self.fail(e)

