# General libraries for DB
import getpass
from typing import Optional
from sqlalchemy import Column, Text, DateTime, ForeignKey, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from core.utils.general_helper import random_code

# Complementary Resources
from datetime import datetime

# ORM Base Class
Base = declarative_base()

# Schema
schema = 'public'

# Class
class Organization(Base):
    __tablename__ = 'organization'
    __table_args__ = {'schema': schema}

    org_code = Column(Text, primary_key=True)
    parent_org = Column(Text, nullable=True)
    org_desc = Column(Text, nullable=False)
    gei_code = Column(Text, nullable=False)
    active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_by_id = Column(Text, default=getpass.getuser())
    updated_by_id = Column(Text, default=getpass.getuser())
    lineage_metadata = Column(JSONB)

    people = relationship('Person', back_populates='titles')
    careers = relationship('Career', back_populates='titles')

    def __init__(self, person_code, career_code, title_desc, lineage_metadata, active:Optional[bool]=True):
        self.title_code = random_code()
        self.person_code = person_code
        self.career_code = career_code,
        self.title_desc = title_desc,
        self.lineage_metadata = lineage_metadata,
        self.active = active,
        self.updated_at = datetime.utcnow(),
        self.updated_by_id = getpass.getuser()

    def to_dict(self):
        return {
            'title_code': self.title_code,
            'person_code': self.person_code,
            'career_code': self.career_code,
            'title_desc': self.title_desc,
            'active': self.active,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'created_by_id': self.created_by_id,
            'updated_by_id': self.updated_by_id,
            'lineage_metadata': self.lineage_metadata
        }
