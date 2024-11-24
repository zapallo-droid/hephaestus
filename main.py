import os
import logging
from dotenv import load_dotenv
from typing import Optional
from pipelines.arg_parliament.orchestrator import ArgParliamentPipeline

# Config and Parameters
load_dotenv()
c_path = os.getenv('COSMOS_PATH')
f_path = os.getenv('FRAMEWORKS_PATH')
s_path = os.getcwd()

daedalus_conn = {'db_user': os.getenv('DB_USER'),
                 'db_pass': os.getenv('DB_PASS'),
                 'db_host': os.getenv('DAEDALUS_DEV_HOST'),
                 'db_port': os.getenv('DAEDALUS_DEV_PORT'),
                 'db_name': 'daedalus_dev'}

# Setup logging
logging.basicConfig(level=logging.INFO)

# Functions Definition
def get_pipeline(pipeline_name):

    pipe_dict = {
        'dom001': ArgParliamentPipeline
        #TBD with new pipelines
    }

    return pipe_dict.get(pipeline_name)


def main_extractor(pipeline_name, system_path:Optional[str]=None,
                   cosmos_path:Optional[str]=None,
                   frameworks_path:Optional[str]=None,
                   daedalus_config:Optional[dict]=None):

    pipe = get_pipeline(pipeline_name)

    if pipe is None:
        logging.error(f"{pipeline_name} pipeline not found")
        return
    else:
        logging.info(f"{pipeline_name} running")
        pipe(system_path=system_path, cosmos_path=cosmos_path, frameworks_path=frameworks_path,
             daedalus_config=daedalus_config).run_job()
        logging.info(f"{pipeline_name} ran")


if __name__ == '__main__':
    # Execution -- Extraction
    for pip_key in ['dom001']:
        main_extractor(pipeline_name=pip_key, system_path=s_path, cosmos_path=c_path,
                       frameworks_path=f_path, daedalus_config=daedalus_conn)