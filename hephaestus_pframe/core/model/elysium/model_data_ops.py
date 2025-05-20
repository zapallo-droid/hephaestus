# General libraries for DB
import uuid

from sqlalchemy import Column, Float, Integer, Text, JSON, DateTime, ForeignKey, Boolean
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship, declarative_base
from hephaestus_pframe.core.utils.general_helper import deterministic_code

# Complementary Resources
import base58
import getpass
from typing import Optional
from datetime import datetime

# ORM model Base Class
Base = declarative_base()

# Schema
schema = 'operations'

# General Use Classes
class App(Base):
    __tablename__ = 'app'
    __table_args__ = {'schema': schema}

    app_code = Column(Text, nullable=False, primary_key=True)
    app_name = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)
    created_by = Column(Text, nullable=False, default=getpass.getuser())
    modified_by = Column(Text, nullable=False)

    pipelines = relationship('Pipeline', back_populates='apps')
    jobs = relationship('Job', back_populates='apps')

    def __init__(self, app_name):
        self.app_name = app_name
        self.app_code = deterministic_code(text=self.app_name, max_length=8)
        self.updated_at = datetime.utcnow()
        self.modified_by = getpass.getuser()

    def to_dict(self):
        return {
            'app_code': self.app_code,
            'app_name': self.app_name,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'created_by': self.created_by,
            'modified_by': self.modified_by
        }


class PipelineDomain(Base):
    __tablename__ = 'pipeline_domain'
    __table_args__ = {'schema': schema}

    pipeline_domain_code = Column(Text, nullable=False, primary_key=True)
    pipeline_domain_name = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)
    created_by = Column(Text, nullable=False, default=getpass.getuser())
    modified_by = Column(Text, nullable=False)

    pipelines = relationship('Pipeline', back_populates='pipeline_domains')

    def __init__(self, pipeline_domain_name):
        self.pipeline_domain_name = pipeline_domain_name
        self.pipeline_domain_code = deterministic_code(text=self.pipeline_domain_name, max_length=8)
        self.updated_at = datetime.utcnow()
        self.modified_by = getpass.getuser()

    def to_dict(self):
        return {
            'pipeline_domain_code': self.pipeline_domain_code,
            'pipeline_domain_name': self.pipeline_domain_name,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'created_by': self.created_by,
            'modified_by': self.modified_by
        }


class Pipeline(Base):
    __tablename__ = 'pipeline'
    __table_args__ = {'schema': schema}

    pipeline_code = Column(Text, nullable=False, primary_key=True)
    pipeline_name = Column(Text, nullable=False)
    pipeline_domain_code = Column(Text, ForeignKey(f'{schema}.pipeline_domain.pipeline_domain_code') ,
                                  nullable=False)
    app_code = Column(Text, ForeignKey(f'{schema}.app.app_code'), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)
    created_by = Column(Text, nullable=False, default=getpass.getuser())
    modified_by = Column(Text, nullable=False, default=getpass.getuser())

    apps = relationship('App', back_populates='examples')
    tasks = relationship('Task', back_populates='examples')
    sources = relationship('Source', back_populates='examples')
    pipeline_domains = relationship('PipelineDomain', back_populates='examples')

    def __init__(self, app_code, pipeline_domain_code, pipeline_name):
        self.pipeline_name = pipeline_name
        self.pipeline_code = deterministic_code(text=self.pipeline_name, max_length=8)
        self.app_code = app_code
        self.pipeline_domain_code = pipeline_domain_code
        self.updated_at = datetime.utcnow()
        self.modified_by = getpass.getuser()

    def to_dict(self):
        return {            
            'app_code': self.app_code,
            'pipeline_domain_code': self.pipeline_domain_code,
            'pipeline_code': self.pipeline_code,
            'pipeline_name': self.pipeline_name,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'created_by': self.created_by,
            'modified_by': self.modified_by
        }


# General Use Classes
class Source(Base):
    __tablename__ = 'source'
    __table_args__ = {'schema': schema}

    source_code = Column(Text, nullable=False, primary_key=True)
    source_name = Column(Text, nullable=False)
    location_type = Column(Text, nullable=False)
    location = Column(Text, nullable=True) # URL
    location_endpoint = Column(Text, nullable=True) # Only for API
    extension = Column(Text, nullable=True)
    extract_type = Column(Text, nullable=False)
    params = Column(JSON, nullable=True)
    headers = Column(JSON, nullable=True)
    timeout = Column(Float, nullable=True)
    pipeline_code = Column(Text, ForeignKey(f'{schema}.pipeline.pipeline_code'), nullable=False)
    active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)
    created_by = Column(Text, nullable=False, default=getpass.getuser())
    modified_by = Column(Text, nullable=False, default=getpass.getuser())

    pipelines = relationship('Pipeline', back_populates='sources')
    tasks = relationship('Task', back_populates='sources')
    
    def __init__(self, source_name, location_type, location, extract_type, pipeline_code,
                 location_endpoint:Optional[str]=None,  extension:Optional[str]=None, params:Optional[dict]=None,
                 headers:Optional[dict]=None, timeout:Optional[int]=None, active:Optional[bool]=True):
        self.source_name = source_name
        self.source_code = deterministic_code(text=self.source_name)
        self.location_type = location_type
        self.location = location
        self.location_endpoint = location_endpoint
        self.extension = extension
        self.extract_type = extract_type
        self.params = params
        self.headers = headers
        self.timeout = timeout
        self.pipeline_code = pipeline_code
        self.active = active
        self.updated_at = datetime.utcnow()
        self.modified_by = getpass.getuser()

    def to_dict(self):
        return {
            'source_code': self.source_code,
            'source_name': self.source_name,
            'location_type': self.location_type,
            'location_endpoint': self.location_endpoint,
            'location': self.location,
            'extension': self.extension,
            'extract_type': self.extract_type,
            'params': self.params,
            'headers': self.headers,
            'timeout': self.timeout,
            'pipeline_code': self.pipeline_code,
            'active': self.active,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'created_by': self.created_by,
            'modified_by': self.modified_by
        }


class Job(Base):
    __tablename__ = 'job'
    __table_args__ = {'schema': schema}

    job_id = Column(UUID(as_uuid=True), primary_key=True)
    job_code = Column(Text, nullable=False, unique=True)
    name = Column(Text, nullable=False)
    memory_usage_start = Column(Float, nullable=False)
    cpu_usage_start = Column(Float, nullable=False)
    memory_usage_end = Column(Float, nullable=False)
    cpu_usage_end = Column(Float, nullable=False)
    status = Column(Text, nullable=False)
    exception = Column(Text, nullable=True)
    started_at = Column(DateTime, nullable=False)
    ended_at = Column(DateTime, nullable=True)
    duration = Column(Float, nullable=True)
    memory_usage = Column(Float, nullable=True)
    cpu_usage = Column(Float, nullable=True)
    host_name = Column(Text, nullable=False)
    execution_user = Column(Text, nullable=False)
    process_id = Column(Integer, nullable=False)
    number_of_tasks = Column(Integer, nullable=False)
    app_code = Column(Text, ForeignKey(f'{schema}.app.app_code'), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)
    created_by = Column(Text, nullable=False, default=getpass.getuser())
    modified_by = Column(Text, nullable=False, default=getpass.getuser())

    apps = relationship('App', back_populates='jobs')
    tasks = relationship('Task', back_populates='jobs')
    
    def __init__(self, job_id, name, memory_usage_start, cpu_usage_start, memory_usage_end, cpu_usage_end, status, exception, 
                 started_at, ended_at, duration, memory_usage, cpu_usage, host_name, execution_user, process_id, 
                 number_of_tasks, app_code):
        self.job_id = job_id
        self.job_code = base58.b58encode(self.job_id.bytes).decode('utf-8')
        self.name = name
        self.memory_usage_start = memory_usage_start
        self.cpu_usage_start = cpu_usage_start
        self.memory_usage_end = memory_usage_end
        self.cpu_usage_end = cpu_usage_end
        self.status = status
        self.exception = exception
        self.started_at = started_at
        self.ended_at = ended_at
        self.duration = duration
        self.memory_usage = memory_usage
        self.cpu_usage = cpu_usage
        self.host_name = host_name
        self.execution_user = execution_user
        self.process_id = process_id
        self.number_of_tasks = number_of_tasks
        self.app_code = app_code
        self.updated_at = datetime.utcnow()
        self.modified_by = getpass.getuser()
    
    def to_dict(self):
        return {
            'job_id': self.job_id,
            'name': self.name,
            'memory_usage_start': self.memory_usage_start,
            'cpu_usage_start': self.cpu_usage_start,
            'memory_usage_end': self.memory_usage_end,
            'cpu_usage_end': self.cpu_usage_end,
            'status': self.status,
            'exception': self.exception,
            'started_at': self.started_at,
            'ended_at': self.ended_at,
            'duration': self.duration,
            'memory_usage': self.memory_usage,
            'cpu_usage': self.cpu_usage,
            'host_name': self.host_name,
            'execution_user': self.execution_user,
            'process_id': self.process_id,
            'number_of_tasks': self.number_of_tasks,
            'app_code': self.app_code,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'created_by': self.created_by,
            'modified_by': self.modified_by
        }


class TaskType(Base):
    __tablename__ = 'task_type'
    __table_args__ = {'schema': schema}

    task_type_code = Column(Text, primary_key=True)
    task_type_name = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)
    created_by = Column(Text, nullable=False, default=getpass.getuser())
    modified_by = Column(Text, nullable=False, default=getpass.getuser())

    tasks = relationship('Task', back_populates='task_types')
    
    def __init__(self, task_type_code, task_type_name):
        self.task_type_code = task_type_code
        self.task_type_name = task_type_name
        self.updated_at = datetime.utcnow()
        self.modified_by = getpass.getuser()

    def to_dict(self):
        return {
            'task_type_code': self.task_type_code,
            'task_type_name': self.task_type_name,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'created_by': self.created_by,
            'modified_by': self.modified_by
        }


class Task(Base):
    __tablename__ = 'task'
    __table_args__ = {'schema': schema}

    task_id = Column(UUID(as_uuid=True), primary_key=True)
    task_code = Column(Text, nullable=False, unique=True)
    name = Column(Text, nullable=False)
    source_code = Column(Text, ForeignKey(f'{schema}.source.source_code'), nullable=False)
    location = Column(Text, nullable=True)
    memory_usage_start = Column(Float, nullable=False)
    cpu_usage_start = Column(Float, nullable=False)
    memory_usage_end = Column(Float, nullable=False)
    cpu_usage_end = Column(Float, nullable=False)
    status = Column(Text, nullable=False)
    location_status = Column(Float, nullable=True)
    task_image = Column(Text, nullable=True)
    task_image_status = Column(Text, nullable=True)
    exception = Column(Text, nullable=True)
    started_at = Column(DateTime, nullable=False)
    ended_at = Column(DateTime, nullable=True)
    duration = Column(Float, nullable=True)
    records_processed = Column(Integer, nullable=True)
    files_processed = Column(Integer, nullable=True)
    memory_usage = Column(Float, nullable=True)
    cpu_usage = Column(Float, nullable=True)
    job_id = Column(UUID, ForeignKey(f'{schema}.job.job_id'), nullable=False)
    pipeline_code = Column(Text, ForeignKey(f'{schema}.pipeline.pipeline_code'), nullable=False)
    task_type_code = Column(Text, ForeignKey(f'{schema}.task_type.task_type_code'), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)
    created_by = Column(Text, nullable=False, default=getpass.getuser())
    modified_by = Column(Text, nullable=False, default=getpass.getuser())

    jobs = relationship('Job', back_populates='tasks')
    pipelines = relationship('Pipeline', back_populates='tasks')
    sources = relationship('Source', back_populates='tasks')
    task_types = relationship('TaskType', back_populates='tasks')
    audit_logs = relationship('AuditLog', back_populates='tasks')
    
    def __init__(self, task_id, name, source_code, location, memory_usage_start, cpu_usage_start, 
                 memory_usage_end,  cpu_usage_end, status, location_status, task_image, task_image_status, exception, 
                 started_at, ended_at, duration, records_processed, files_processed, memory_usage, cpu_usage, job_id,
                 pipeline_code, task_type_code):
        self.task_id = task_id
        self.task_code = base58.b58encode(self.task_id.bytes).decode('utf-8')
        self.name = name
        self.source_code = source_code
        self.location = location
        self.memory_usage_start = memory_usage_start
        self.cpu_usage_start = cpu_usage_start
        self.memory_usage_end = memory_usage_end
        self.cpu_usage_end = cpu_usage_end
        self.status = status
        self.location_status = location_status
        self.task_image = task_image
        self.task_image_status = task_image_status
        self.exception = exception
        self.started_at = started_at
        self.ended_at = ended_at
        self.duration = duration
        self.records_processed = records_processed
        self.files_processed = files_processed
        self.memory_usage = memory_usage
        self.cpu_usage = cpu_usage
        self.job_id = job_id
        self.pipeline_code = pipeline_code
        self.task_type_code = task_type_code
        self.updated_at = datetime.utcnow()
        self.modified_by = getpass.getuser()

    def to_dict(self):
        return {
            'task_id': self.task_id,
            'task_code': self.task_code,
            'name': self.name,
            'location': self.location,
            'memory_usage_start': self.memory_usage_start,
            'cpu_usage_start': self.cpu_usage_start,
            'memory_usage_end': self.memory_usage_end,
            'cpu_usage_end': self.cpu_usage_end,
            'status': self.status,
            'location_status': self.location_status,
            'task_image': self.task_image,
            'task_image_status': self.task_image_status,
            'exception': self.exception,
            'started_at': self.started_at,
            'ended_at': self.ended_at,
            'duration': self.duration,
            'records_processed': self.records_processed,
            'files_processed': self.files_processed,
            'memory_usage': self.memory_usage,
            'cpu_usage': self.cpu_usage,
            'job_id': self.job_id,
            'pipeline_code': self.pipeline_code,
            'task_type_code': self.task_type_code,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'created_by': self.created_by,
            'modified_by': self.modified_by
        }


class OperationType(Base):
    __tablename__ = 'operation_type'
    __table_args__ = {'schema': schema}

    operation_type_code = Column(Text, primary_key=True)
    operation_type_name = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)
    created_by = Column(Text, nullable=False, default=getpass.getuser())
    modified_by = Column(Text, nullable=False, default=getpass.getuser())

    audit_logs = relationship('AuditLog', back_populates='operation_types')

    def __init__(self, task_type_code, task_type_name):
        self.task_type_code = task_type_code
        self.task_type_name = task_type_name
        self.updated_at = datetime.utcnow()
        self.modified_by = getpass.getuser()

    def to_dict(self):
        return {
            'task_type_code': self.task_type_code,
            'task_type_name': self.task_type_name,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'created_by': self.created_by,
            'modified_by': self.modified_by
        }


class AuditLog(Base):
    __tablename__ = 'audit_logs'
    __table_args__ = {'schema': schema}

    log_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    table_name = Column(Text, nullable=False)
    record_id = Column(Text, nullable=False)
    task_id = Column(UUID, ForeignKey(f'{schema}.task.task_id'), nullable=False)
    operation_type_code = Column(Text, ForeignKey(f'{schema}.operation_type.operation_type_code'), nullable=False)
    previous_value = Column(JSONB, nullable=False)
    new_value = Column(JSONB, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)
    created_by = Column(Text, nullable=False, default=getpass.getuser())
    modified_by = Column(Text, nullable=False, default=getpass.getuser())

    tasks = relationship('Task', back_populates='audit_logs')
    operation_types = relationship('OperationType', back_populates='audit_logs')
    
    def __init__(self, task_id, table_name, record_id, operation_type_code, previous_value, new_value):
        self.task_id = task_id
        self.table_name = table_name
        self.record_id = record_id
        self.operation_type_code = operation_type_code        
        self.previous_value = previous_value
        self.new_value = new_value
        self.updated_at = datetime.utcnow()
        self.modified_by = getpass.getuser()

    def to_dict(self):
        return {
            'log_id': self.log_id,
            'table_name': self.table_name,
            'record_id': self.record_id,
            'task_id': self.task_id,
            'operation_type_code': self.operation_type_code,
            'previous_value': self.previous_value,
            'new_value': self.new_value,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'created_by': self.created_by,
            'modified_by': self.modified_by
        }

