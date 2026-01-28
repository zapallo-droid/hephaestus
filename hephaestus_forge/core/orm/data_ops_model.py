# --General Libraries--
import uuid
import os
from typing import Optional, List
from enum import Enum as EnumEnum

from sqlalchemy import Integer, Text, UUID, ForeignKey, Float, Boolean, Enum
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.ext.mutable import MutableDict

# --Project Resources--
from hephaestus_forge.core.orm.base import Base
from hephaestus_forge.core.orm.data_ops_helpers import (default_stats, default_environment_metadata, 
                                                        default_app_version)

# --Script Resources--
SCHEMA = os.getenv('HEPHAESTUS_SCHEMA', 'data_ops')


# --Enum Classes--
class TaskTypeEnum(EnumEnum):
    # ETL Specific
    EXTRACT = 'extract'
    TRANSFORM = 'transform'
    LOAD = 'load'
    # Other Frameworks
    ANALYZE = 'analyze'
    AGENT = 'agent' # Operator
    TRANSFER = 'transfer' # Files Transfer

class OperationsTypeEnum(EnumEnum):
    CREATE = 'create'
    UPDATE = 'update'
    DELETE = 'delete'

class StatusEnum(EnumEnum):
    PENDING = 'pending'
    STARTED = 'started'
    FAILED = 'failed'
    FINISHED = 'finished'

class TransferTypeEnum(EnumEnum):
    FETCH = 'fetch'
    MOVEMENT = 'movement'


# --DataOps Models--
class AppORM(Base):
    __tablename__ = 'app'
    __table_args__ = {'schema': SCHEMA}

    ## --ID-- ##
    app_code: Mapped[str] = mapped_column(Text, primary_key=True)
    app_name: Mapped[str] = mapped_column(Text, nullable=False)

    ## --RELATIONSHIPS-- ##
    jobs: Mapped[List["JobORM"]] = relationship("JobORM", back_populates="app")
    pipelines: Mapped[List["PipelineORM"]] = relationship("PipelineORM", back_populates="app")


class PipelineDomainORM(Base):
    __tablename__ = 'pipeline_domain'
    __table_args__ = {'schema': SCHEMA}

    ## --ID-- ##
    pipeline_domain_code: Mapped[str] = mapped_column(Text, primary_key=True)
    pipeline_domain_name: Mapped[str] = mapped_column(Text, nullable=False)

    ## --RELATIONSHIPS-- ##
    pipelines: Mapped[List["PipelineORM"]] = relationship("PipelineORM", back_populates="pipeline_domain")


class PipelineORM(Base):
    __tablename__ = 'pipeline'
    __table_args__ = {'schema': SCHEMA}

    ## --ID-- ##
    pipeline_code: Mapped[str] = mapped_column(Text, primary_key=True)
    pipeline_name: Mapped[str] = mapped_column(Text, nullable=False)
    pipeline_domain_code: Mapped[str] = mapped_column(Text, ForeignKey(f'{SCHEMA}.pipeline_domain.pipeline_domain_code'), nullable=False)
    app_code: Mapped[str] = mapped_column(Text, ForeignKey(f'{SCHEMA}.app.app_code'), nullable=False)

    ## --RELATIONSHIPS-- ##
    pipeline_domain: Mapped["PipelineDomainORM"] = relationship("PipelineDomainORM", back_populates="pipelines")
    app: Mapped["AppORM"] = relationship("AppORM", back_populates="pipelines")
    sources: Mapped[List["SourceORM"]] = relationship("SourceORM", back_populates="pipeline")
    tasks: Mapped[List["TaskORM"]] = relationship("TaskORM", back_populates="pipeline")


class SourceORM(Base):
    __tablename__ = 'source'
    __table_args__ = {'schema': SCHEMA}

    ## --ID-- ##
    source_code: Mapped[str] = mapped_column(Text, primary_key=True)
    source_name: Mapped[str] = mapped_column(Text, nullable=False)

    ## --CONFIG-- ##
    location_type: Mapped[str] = mapped_column(Text, nullable=False)
    location: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    location_endpoint: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    extension: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    extract_type: Mapped[str] = mapped_column(Text, nullable=False)
    params: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    pagination_params: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    headers: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    timeout: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    pipeline_code: Mapped[str] = mapped_column(Text, ForeignKey(f'{SCHEMA}.pipeline.pipeline_code'), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    comments: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    ## --RELATIONSHIPS-- ##
    pipeline: Mapped["PipelineORM"] = relationship("PipelineORM", back_populates="sources")
    transfer_logs: Mapped[List["TransferLogORM"]] = relationship("TransferLogORM", back_populates="source")


class JobORM(Base):
    __tablename__ = 'job'
    __table_args__ = {'schema': SCHEMA}

    ## --ID-- ##
    job_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_name: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[StatusEnum] = mapped_column(Enum(StatusEnum, name='status_enum', schema=SCHEMA), nullable=False)
    exception: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    app_code: Mapped[str] = mapped_column(Text, ForeignKey(f'{SCHEMA}.app.app_code'), nullable=False)
    app_version: Mapped[dict] = mapped_column(JSONB, nullable=False, default=default_app_version)
    app_hash: Mapped[str] = mapped_column(Text, nullable=False)
    job_hash: Mapped[str] = mapped_column(Text, nullable=False)

    ## --STATS-- ##
    job_stats: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSONB), nullable=False, default=default_stats)
    number_of_tasks: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    ## --METADATA-- ##
    environment_metadata: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSONB), nullable=False, default=default_environment_metadata)
    job_digest: Mapped[str] = mapped_column(Text, nullable=True)

    ## --RELATIONSHIPS-- ##
    app: Mapped["AppORM"] = relationship("AppORM", back_populates="jobs")
    tasks: Mapped[List["TaskORM"]] = relationship("TaskORM", back_populates="job")


class TaskORM(Base):
    __tablename__ = 'task'
    __table_args__ = {'schema': SCHEMA}

    ## --ID-- ##   
    task_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    task_name: Mapped[str] = mapped_column(Text, nullable=False)    

    ## --STATUS-- ##
    status: Mapped[StatusEnum] = mapped_column(Enum(StatusEnum, name='status_enum', schema=SCHEMA), nullable=False) 
    exception: Mapped[str] = mapped_column(Text, nullable=True)    

    ## --STATS-- ##     
    task_stats: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSONB), nullable=False, default=default_stats)
    records_processed: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    files_processed: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    ## --METADATA-- ##    
    job_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey(f'{SCHEMA}.job.job_id'), nullable=False)
    pipeline_code: Mapped[str] = mapped_column(Text, ForeignKey(f'{SCHEMA}.pipeline.pipeline_code'), nullable=False)
    task_type: Mapped[TaskTypeEnum] = mapped_column(Enum(TaskTypeEnum, name='task_type_enum', schema=SCHEMA), nullable=False)
    task_digest: Mapped[str] = mapped_column(Text, nullable=True)

    ## --RELATIONSHIPS-- ##
    job: Mapped["JobORM"] = relationship("JobORM", back_populates="tasks")
    pipeline: Mapped["PipelineORM"] = relationship("PipelineORM", back_populates="tasks")
    audit_logs: Mapped[List["AuditLogORM"]] = relationship("AuditLogORM", back_populates="task")
    transfer_logs: Mapped[List["TransferLogORM"]] = relationship("TransferLogORM", back_populates="task")


class AuditLogORM(Base):
    __tablename__ = 'audit_log'
    __table_args__ = {'schema': SCHEMA}

    ## --ID-- ##
    log_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    task_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey(f'{SCHEMA}.task.task_id'), nullable=False)
    
    ## --LOG-- ##
    table_name: Mapped[str] = mapped_column(Text, nullable=False)
    record_id: Mapped[str] = mapped_column(Text, nullable=False)    
    operation_type: Mapped[OperationsTypeEnum] = mapped_column(Enum(OperationsTypeEnum, name='operations_type_enum', schema=SCHEMA), nullable=False)

    ## --METADATA-- ##
    previous_value: Mapped[dict] = mapped_column(JSONB, nullable=True, default=dict)
    new_value: Mapped[dict] = mapped_column(JSONB, nullable=False)

    ## --RELATIONSHIPS-- ##
    task: Mapped["TaskORM"] = relationship("TaskORM", back_populates="audit_logs")



class TransferLogORM(Base):
    __tablename__ = 'transfer_log'
    __table_args__ = {'schema': SCHEMA}

    ## --ID-- ##
    log_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    task_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey(f'{SCHEMA}.task.task_id'), nullable=False)
    source_code: Mapped[str] = mapped_column(Text, ForeignKey(f'{SCHEMA}.source.source_code'), nullable=True)

    ## --LOG-- ##
    input_path: Mapped[str] = mapped_column(Text, nullable=False)
    output_path: Mapped[str] = mapped_column(Text, nullable=False)
    input_format: Mapped[str] = mapped_column(Text, nullable=False)
    output_format: Mapped[str] = mapped_column(Text, nullable=False)
    
    transfer_type: Mapped[TransferTypeEnum] = mapped_column(Enum(TransferTypeEnum, name='transfer_type_enum', schema=SCHEMA), nullable=False)

    ## --RELATIONSHIPS-- ##
    task: Mapped["TaskORM"] = relationship("TaskORM", back_populates="transfer_logs")
    source: Mapped["SourceORM"] = relationship("SourceORM", back_populates="transfer_logs")

