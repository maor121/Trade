from dataclasses import dataclass
import datetime
from typing import List

import sqlalchemy
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Integer, String, Boolean
from sqlalchemy.ext.hybrid import hybrid_property

db = SQLAlchemy()


@dataclass
class FBPost(db.Model):
    """Local Device table in DB"""
    __tablename__ = "FBPost"
    __table_args__ = {'extend_existing': True}
    post_id: int = Column(Integer, primary_key=True, unique=True,
                          autoincrement=False, nullable=False)
    post_text: str = Column(String, nullable=True)


@dataclass
class FBGroup(db.Model):
    __tablename__ = "FBGroup"
    __table_args__ = {'extend_existing': True}
    group_id: int = Column(Integer, primary_key=True, unique=True,
                           autoincrement=False, nullable=False)

    is_private: bool = Column(Boolean, nullable=False)
    _city_codes: str = Column(String, nullable=False)

    # Scraper stuff
    should_scrape: bool = Column(Boolean, nullable=False)
    last_scrape_date: datetime.datetime = \
        Column(sqlalchemy.DateTime(timezone=True), nullable=True)

    @hybrid_property
    def city_codes(self):
        return [int(c) for c in self._city_codes.split("|")]

    @city_codes.setter
    def city_codes(self, val: List[int]):
        val = [str(v) for v in val]
        self._city_codes = "|".join(val)

