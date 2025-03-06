import os
import logging
from dotenv import load_dotenv
from typing import Optional
from pipelines.arg_parliament.orchestrator import ArgParliamentPipeline
from pipelines.ilostat.orchestrator import IlostatPipeline
from pipelines.arg_census.orchestrator import ARGCensusPipeline
from pipelines.geopolitical_entities.orchestrator import ISOGeopoliticalEntities

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
        'dom001': ArgParliamentPipeline,
        'dom002': IlostatPipeline,
        'dom003': ARGCensusPipeline,
        'dom004': ISOGeopoliticalEntities
        #TBD with new pipelines
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


if __name__ == '__main__':
    # Execution -- Extraction
    #for pip_key in ['dom001','dom002','dom003','dom004']:
    for pip_key in ['dom003']:
        main_extractor(pipeline_name=pip_key, system_path=s_path, cosmos_path=c_path,
                       elysium_config=elysium_conn)