# General libraries for DB
import getpass
from sqlalchemy import Column, Text, DateTime, ForeignKey, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base

# Complementary Resources
from datetime import datetime

# ORM Base Class
Base = declarative_base()

# Schema
schema = 'public'

# Class
class GeopoliticalEntity(Base):
    __tablename__ = 'geopolitical_entity'
    __table_args__ = {'schema': schema}

    iso3_code = Column(Text, primary_key=True)
    iso2_code = Column(Text, nullable=False, unique=True)
    un_code = Column(Text, nullable=False, unique=True)
    ilo_member = Column(Boolean, nullable=True)
    name = Column(Text, nullable=False)
    official_lang_code = Column(Text, nullable=False)
    geo_point_2d = Column(JSONB, nullable=False)
    geo_shape_metadata = Column(JSONB, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_by_id = Column(Text, default=getpass.getuser())
    updated_by_id = Column(Text, default=getpass.getuser())
    lineage_metadata = Column(JSONB)

    geopolitical_sub_entities = relationship('GeopoliticalSubEntity', back_populates='geopolitical_entities')

    def __init__(self, iso3_code, iso2_code, un_code, ilo_member, name, official_lang_code,
                 geo_point_2d,geo_shape_metadata):
        self.iso3_code = iso3_code,
        self.iso2_code = iso2_code,
        self.un_code = un_code,
        self.ilo_member = ilo_member,
        self.name = name,
        self.official_lang_code = official_lang_code,
        self.geo_point_2d = geo_point_2d,
        self.geo_shape_metadata = geo_shape_metadata
        self.updated_at = datetime.utcnow(),
        self.updated_by_id = getpass.getuser()

    def to_dict(self):
        return {
            'iso3_code': self.iso3_code,
            'iso2_code': self.iso2_code,
            'un_code': self.un_code,
            'ilo_member': self.ilo_member,
            'name': self.name,
            'official_lang_code': self.official_lang_code,
            'geo_point_2d': self.geo_point_2d,
            'geo_shape_metadata': self.geo_shape_metadata,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'created_by_id': self.created_by_id,
            'updated_by_id': self.updated_by_id,
            'lineage_metadata': self.lineage_metadata
        }


class GeopoliticalSubEntity(Base):
    __tablename__ = 'geopolitical_sub_entity'
    __table_args__ = {'schema': schema}

    gse_iso_code = Column(Text, primary_key=True)
    ge_iso_code = Column(Text, ForeignKey(f'{schema}.geopolitical_entity.iso3_code'), nullable=False)
    name = Column(Text, nullable=True)
    category = Column(Text, nullable=False)
    geo_point_2d = Column(JSONB, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_by_id = Column(Text, default=getpass.getuser())
    updated_by_id = Column(Text, default=getpass.getuser())
    lineage_metadata = Column(JSONB)

    geopolitical_entities = relationship('GeopoliticalEntity', back_populates='geopolitical_sub_entities')

    def __init__(self, gse_iso_code, ge_iso_code, name, category, geo_point_2d):
        self.gse_iso_code = gse_iso_code
        self.ge_iso_code = ge_iso_code,
        self.name = name,
        self.category = category,
        self.geo_point_2d = geo_point_2d,
        self.updated_at = datetime.utcnow(),
        self.updated_by_id = getpass.getuser()

    def to_dict(self):
        return {
            'gse_iso_code': self.gse_iso_code,
            'ge_iso_code': self.ge_iso_code,
            'name': self.name,
            'category': self.category,
            'geo_point_2d': self.geo_point_2d,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'created_by_id': self.created_by_id,
            'updated_by_id': self.updated_by_id,
            'lineage_metadata': self.lineage_metadata
        }