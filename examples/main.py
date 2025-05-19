import os
import logging
from dotenv import load_dotenv
from typing import Optional

# Config and Parameters
load_dotenv()
c_path = os.getenv('COSMOS_PATH')
s_path = os.getcwd()

environment = 'DEV'
elysium_conn = {'db_user': os.getenv('DB_USER'),
                'db_pass': os.getenv('DB_PASS'),
                'db_host': os.getenv(f'{environment.upper()}_HOST'),
                'db_port': os.getenv(f'{environment.upper()}_PORT'),
                'db_name': f'elysium_{environment.lower()}'}

# Setup logging
logging.basicConfig(level=logging.INFO)

# Functions Definition
def get_pipeline(pipeline_name):

    pipe_dict = {
        'dom001': None, ## Domain I (PIPELINE)
        'dom002': None, ## Domain I (PIPELINE)
        'dom003': None, ## Domain I (PIPELINE)
        'dom004': None ## Domain I (PIPELINE)
        #TBD with new examples
    }

    return pipe_dict.get(pipeline_name)


def main_extractor(pipeline_name, system_path:Optional[str]=None,
                   cosmos_path:Optional[str]=None,
                   elysium_config:Optional[dict]=None):

    pipe = get_pipeline(pipeline_name)

    if pipe is None:
        logging.error(f"{pipeline_name} pipeline not found")
        return
    else:
        logging.info(f"{pipeline_name} running")
        pipe(system_path=system_path, cosmos_path=cosmos_path, db_config=elysium_config).run_job()
        logging.info(f"{pipeline_name} ran")
