# General libraries for DB
import getpass
from sqlalchemy import Column, Text, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

# Complementary Resources
from datetime import datetime

# ORM Base Class
Base = declarative_base()

# Schema
schema = 'public'

# Class
class Role(Base):
    __tablename__ = 'role'
    __table_args__ = {'schema':schema}

    role_code = Column(Text, primary_key=True)
    role_name = Column(Text)
    role_code_l4 = Column(Text)
    role_name_l4 = Column(Text)
    role_code_l3 = Column(Text)
    role_name_l3 = Column(Text)
    role_code_l2 = Column(Text)
    role_name_l2 = Column(Text)
    role_code_l1 = Column(Text)
    role_name_l1 = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_by_id = Column(Text, default=getpass.getuser())
    updated_by_id = Column(Text, default=getpass.getuser())
    lineage_metadata = Column(JSONB)

    def to_dict(self):
        return {
            'role_code':self.role_code,
            'role_name':self.role_name,
            'role_code_l4':self.role_code_l4,
            'role_name_l4':self.role_name_l4,
            'role_code_l3':self.role_code_l3,
            'role_name_l3':self.role_name_l3,
            'role_code_l2':self.role_code_l2,
            'role_name_l2':self.role_name_l2,
            'role_code_l1':self.role_code_l1,
            'role_name_l1':self.role_name_l1,
            'created_at':self.created_at,
            'created_by_id':self.created_by_id,
            'updated_at':self.updated_at,
            'updated_by_id':self.updated_by_id,
            'lineage_metadata':self.metadata
        }

    @classmethod
    def from_dict(cls, data:dict):
        return cls(
            role_code = data.get('role_code'),
            role_name = data.get('role_name'),
            role_code_l4 = data.get('role_code_l4'),
            role_name_l4 = data.get('role_name_l4'),
            role_code_l3 = data.get('role_code_l3'),
            role_name_l3 = data.get('role_name_l3'),
            role_code_l2 = data.get('role_code_l2'),
            role_name_l2 = data.get('role_name_l2'),
            role_code_l1 = data.get('role_code_l1'),
            role_name_l1 = data.get('role_name_l1'),
            updated_at=Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
            updated_by_id = Column(Text, default=getpass.getuser()),
            lineage_metadata = Column(JSONB)
        )