from dataclasses import dataclass

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Integer, String

db = SQLAlchemy()


@dataclass
class FacebookPost(db.Model):
    """Local Device table in DB"""
    __tablename__ = "FacebookPost"
    __table_args__ = {'extend_existing': True}
    post_id: int = Column(Integer, primary_key=True, unique=True,
                          autoincrement=False, nullable=False)
    post_text: str = Column(String, nullable=True)
