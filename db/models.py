from dataclasses import dataclass
import datetime
from typing import List

import sqlalchemy
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, String, Boolean, ForeignKey, BigInteger, Integer
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship, Mapped

db = SQLAlchemy()


@dataclass
class FBGroup(db.Model):
    __tablename__ = "FBGroup"
    __table_args__ = {'extend_existing': True}
    group_id: int = Column(BigInteger, primary_key=True, unique=True,
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


@dataclass
class FBPost(db.Model):
    __tablename__ = "FBPost"
    __table_args__ = {'extend_existing': True}
    post_id: int = Column(BigInteger, primary_key=True, unique=True,
                          autoincrement=False, nullable=False)
    post_text: str = Column(String, nullable=True)

    group_id: int = Column(BigInteger, ForeignKey("FBGroup.group_id"))
    group: Mapped[FBGroup] = relationship("FBGroup", lazy="joined")


@dataclass
class User(db.Model):
    __tablename__ = "User"
    __table_args__ = {'extend_existing': True}
    id: int = Column(Integer, primary_key=True, unique=True,
                     autoincrement=True, nullable=False)

    name: str = Column(String, nullable=False)

    _search_city_codes: str = Column(String, nullable=False)

    @hybrid_property
    def search_city_codes(self):
        return [int(c) for c in self._search_city_codes.split("|")]

    @search_city_codes.setter
    def search_city_codes(self, val: List[int]):
        val = [str(v) for v in val]
        self._search_city_codes = "|".join(val)


@dataclass
class UserFBPosts(db.Model):
    __tablename__ = "UserNotifiedFBPosts"
    __table_args__ = {'extend_existing': True}
    id: int = Column(Integer, primary_key=True, unique=True,
                     autoincrement=True, nullable=False)

    user_id: int = Column(Integer, ForeignKey("User.id"))
    post_id: int = Column(BigInteger, ForeignKey("FBPost.post_id"))

    is_relevant: bool = Column(Boolean, nullable=False)
    is_notified: bool = Column(Boolean, nullable=False)

    user: Mapped[User] = relationship("User", lazy="joined")
    post: Mapped[FBPost] = relationship("FBPost", lazy="joined")
