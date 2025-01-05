# General libraries for DB
import getpass
from sqlalchemy import Column, Float, Integer, Text, JSON, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import ULID
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError

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
    occupation_code_l4 = Column(Text)
    occupation_name_l4 = Column(Text)
    occupation_code_l3 = Column(Text)
    occupation_name_l3 = Column(Text)
    occupation_code_l2 = Column(Text)
    occupation_name_l2 = Column(Text)
    occupation_code_l1 = Column(Text)
    occupation_name_l1 = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_by_id = Column(Text, getpass.getuser())
    updated_by_id = Column(Text, getpass.getuser())

    def to_dict(self):
        return {
            'role_code':self.role_code,
            'role_name':self.role_name,
            'occupation_code_l4':self.occupation_code_l4,
            'occupation_name_l4':self.occupation_name_l4,
            'occupation_code_l3':self.occupation_code_l3,
            'occupation_name_l3':self.occupation_name_l3,
            'occupation_code_l2':self.occupation_code_l2,
            'occupation_name_l2':self.occupation_name_l2
            'occupation_code_l1':self.occupation_code_l1,
            'occupation_name_l1':self.occupation_name_l1,
            'created_at':self.created_at,
            'updated_at':self.updated_at
        }

    @classmethod
    def from_dict(cls, data:dict):
        return cls(
            role_code = data.get('role_code'),
            role_name = data.get('role_name'),
            occupation_code_l4 = data.get('occupation_code_l4'),
            occupation_name_l4 = data.get('occupation_name_l4'),
            occupation_code_l3 = data.get('occupation_code_l3'),
            occupation_name_l3 = data.get('occupation_name_l3'),
            occupation_code_l2 = data.get('occupation_code_l2'),
            occupation_name_l2 = data.get('occupation_name_l2'),
            occupation_code_l1 = data.get('occupation_code_l1'),
            occupation_name_l1 = data.get('occupation_name_l1'),
            updated_at=Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
            updated_by_id = Column(Text, getpass.getuser())
        )