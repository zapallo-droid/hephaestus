import os
import io
import gzip
import json
import logging
import numpy as np
import pandas as pd
from core.model.jobs import Task
from core.utils.handler_bucket import BucketHandler

class ISCOTransform(Task):
    def __init__(self, job_id:str, name:str, config:dict, bucket_path:str):
        self.bucket_path = bucket_path

        super().__init__(job_id=job_id,
                         name=name,
                         pipeline_code=config.get("pipeline_code"),
                         source_code=config.get("source_code"),
                         task_type_code='T')

        self.config = config
        self.data = None

    def get_base_data(self)->pd.DataFrame:

        try:
            file_path = os.path.join(self.bucket_path, 'raw', str(self.job_id))
            self.location = os.path.join(file_path, os.listdir(file_path)[0])
            if self.location is None:
                e = IndexError(f"No files found in {file_path}")
                self.fail(e)
                raise e

            logging.info(f"Loading data from {file_path}")
            logging.info(f"Extracting compressed file from {self.location}")
            self.task_image = 'io.buffer'

            with gzip.open(self.location, 'rb') as cf:
                decomp_data = cf.read()

            buffer = io.BytesIO(decomp_data)

            data = pd.read_excel(buffer)

            for col in data.columns:
                data[col] = data[col].astype(str)

            data['ISCO 08 Code'] = np.where(
                (data['Level'] == "2") & (data['ISCO 08 Code'].str.len() < 2), data['ISCO 08 Code'].str.zfill(2),
                np.where(
                    (data['Level'] == "3") & (data['ISCO 08 Code'].str.len() < 3), data['ISCO 08 Code'].str.zfill(3),
                    np.where(
                        (data['Level'] == "4") & (data['ISCO 08 Code'].str.len() < 4),
                        data['ISCO 08 Code'].str.zfill(4),
                        data['ISCO 08 Code']
                    )
                )
            )

            return data

        except Exception as e:
            self.fail(e)

    def get_roles_clusters(self, data: pd.DataFrame) -> pd.DataFrame:
        try:
            #roles_dict = data.copy().drop(columns=['Included occupations', 'Excluded occupations'])
            roles_cluster = data[['Level', 'ISCO 08 Code', 'Title EN']].rename(columns={'Level': 'level',
                                                                                        'ISCO 08 Code': 'code',
                                                                                        'Title EN': 'name'})
            roles_cluster['code'] = roles_cluster['code'].astype(str)

            # levels datasets
            l4_data = roles_cluster.loc[roles_cluster['level'] == "4"].copy().drop(columns='level').rename(
                columns={'code': 'role_code_l4',
                         'name': 'role_name_l4'})
            l4_data['role_code_l3'] = l4_data['role_code_l4'].str[:-1]

            l3_data = roles_cluster.loc[roles_cluster['level'] == "3"].copy().drop(columns='level').rename(
                columns={'code': 'role_code_l3',
                         'name': 'role_name_l3'})
            l3_data['role_code_l2'] = l3_data['role_code_l3'].str[:-1]

            l2_data = roles_cluster.loc[roles_cluster['level'] == "2"].copy().drop(columns='level').rename(
                columns={'code': 'role_code_l2',
                         'name': 'role_name_l2'})
            l2_data['role_code_l1'] = l2_data['role_code_l2'].str[:-1]

            l1_data = roles_cluster.loc[roles_cluster['level'] == "1"].copy().drop(columns='level').rename(
                columns={'code': 'role_code_l1',
                         'name': 'role_name_l1'})

            roles_cluster = (
                l4_data.
                merge(l3_data, on='role_code_l3', how='left').
                merge(l2_data, on='role_code_l2', how='left').
                merge(l1_data, on='role_code_l1', how='left')
            )

            roles_cluster = roles_cluster[
                ['role_code_l1', 'role_code_l2', 'role_code_l3', 'role_code_l4', 'role_name_l1', 'role_name_l2',
                 'role_name_l3', 'role_name_l4']].drop_duplicates().reset_index(drop=True)

            return roles_cluster

        except Exception as e:
            self.fail(e)

    def get_data(self) -> list[dict]:
        try:
            data = self.get_base_data()
            roles_cluster = self.get_roles_clusters(data)

            roles = data.copy().drop(
                columns=['Definition', 'Title EN', 'Excluded occupations', 'Tasks include', 'Notes']).rename(
                columns={'Level': 'level',
                         'ISCO 08 Code': 'code',
                         'Included occupations': 'role'})
            roles = roles[roles['level'] == "4"]
            # Keeping only list of roles
            roles['role'] = roles['role'].str.split('\n')  # Splitting by \n
            roles['role'] = roles['role'].str[1:]  # Removing introductory text
            roles = roles.explode('role')  # Explode <3
            roles['role'] = roles['role'].str.strip().str.extract(
                r'([A-Za-z].*)')  # .str.split('-').str[1:] # Cleaning names

            # Cleaning the table
            roles.drop(columns=['level'], inplace=True)
            roles.rename(columns={'code': 'role_code_l4',
                                  'role': 'role_name'}, inplace=True)
            roles['role_code_l4'] = roles['role_code_l4'].astype(str)
            roles['role_code'] = None

            # Creating Codes
            for cluster in roles['role_code_l4'].unique():
                filter_mask = roles['role_code_l4'] == cluster
                roles.loc[filter_mask, 'role_code'] = (
                        roles.loc[filter_mask, 'role_code_l4'].astype(str) +
                        '-' +
                        (roles.loc[filter_mask].reset_index().index + 1).astype(str)
                )

            roles = roles_cluster.merge(roles, on='role_code_l4', how='left')

            return roles.to_dict('records')

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


