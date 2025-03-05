import os
import json
import gzip
import logging
from tqdm import tqdm
from core.model.jobs import Task
from core.utils.handler_bucket import BucketHandler
from core.utils.sql_helper import ConfigSources
from core.model.elysium.resources.geopolitical_entities import GeopoliticalEntity


class GeopoliticalEntitiesTransform(Task):
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

            # Getting raw data through the path handler
            bucket = BucketHandler(self.bucket_path)
            data = bucket.importer(path.replace('.json.gz', ''))

            # Transforming data
            model_col_names = [col.name for col in GeopoliticalEntity.__table__.columns]

            for record in tqdm(data, desc='Processing Geopolitical Entities'):
                record['un_code'] = record.pop('onu_code')
                record['ilo_member'] = record.pop('is_ilomember')
                record['ilo_member'] = True if record['ilo_member'].strip().upper()=='Y' else False
                record['name'] = record.pop('label_en')
                record['geo_shape_metadata'] = record.pop('geo_shape')

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

            if self.data:
                file_name = f"{self.config.get('pipeline_code', '')}_{self.source_code}"
                folder = f'transformed/{self.job_id}'
                file_path = os.path.join(self.bucket_path, folder)

                logging.info(f'Exporting {self.name} to {file_path}')

                self.task_image = os.path.join(file_path, f'{file_name}.json.gz')
                self.records_processed = len(self.data)

                os.makedirs(file_path, exist_ok=True)

                if not self.data:
                    e = Exception("No data transformed")
                    self.fail(e)
                    raise e

                with gzip.open(self.task_image, 'wt', encoding='utf-8') as f:
                    if isinstance(self.data, list) and self.data:
                        try:
                            json.dump(self.data, f, ensure_ascii=False, indent=4)
                        except Exception as e:
                            self.fail(e)
                            raise e
                    else:
                        e = ValueError("Unexpected data format")
                        self.fail(e)
                        raise e

                logging.info(f'{self.records_processed} records, successfully loaded')

            else:
                logging.warning("No data transformed")
                e = Exception("No data transformed")
                self.fail(e)
                raise e

        except Exception as e:
            self.fail(e)

