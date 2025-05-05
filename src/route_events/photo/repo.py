from sqlalchemy import (
    MetaData, 
    Engine, 
    inspect, 
    text,
    delete, 
    Table, 
    Column, 
    Integer, 
    String, 
    Float
)
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from dataclasses import dataclass, asdict
from .photo import SurveyPhoto
from typing import List


Base = declarative_base()
_table_name = 'survey_photo'

@dataclass
class SurveyPhotoORM(Base):
    __tablename__ = _table_name
    url: str = Column(String(1024), primary_key=True)
    sta_meters: float = Column(Float)
    survey_year: int = Column(Integer)
    linkid: str = Column(String(255))
    latitude: float = Column(Float)
    longitude: float = Column(Float)


class SurveyPhotoRepo(object):
    def __init__(self, sql_engine: Engine):
        self._table = _table_name
        self._engine = sql_engine
        self._inspect = inspect(sql_engine)
        self.session = sessionmaker(bind=sql_engine)

    @property
    def table(self):
        return self._table
    
    def _create_table(self):
        """
        Create survey photo table.
        """
      
        Base.metadata.create_all(self._engine)
        
    def insert(self, photos: List[SurveyPhoto], session: Session, commit=False):
        """
        Insert photo into table.
        """
        photos_orm = [
            SurveyPhotoORM(**asdict(photo)) for photo in photos
        ]

        session.bulk_save_objects(photos_orm)

        if commit:
            session.commit()
            
        return
    
    def delete(self, photos: List[SurveyPhoto], session: Session, commit=False):
        """
        Delete photo in the table.
        """
        stmt = delete(SurveyPhotoORM).where(SurveyPhotoORM.url.in_(
            [photo.url for photo in photos]
        ))

        session.execute(stmt)

        if commit:
            session.commit()

        return

