# --General Libraries--
import logging
from sqlalchemy.orm import Session

# --Project Resources--
from hephaestus_forge.core.model.job import Job

# --Core Classes--
class Pipeline:
    def __init__(self, app_code: str, pipeline_code: str, jobs: list[Job]):
        self.app_code = app_code
        self.pipeline_code = pipeline_code
        self.jobs = jobs

    def run(self, session: Session):
        for job in self.jobs:
            job.execute(session=session)