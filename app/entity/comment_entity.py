from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import backref, relationship
from uuid import uuid4

from ..utils.db_connection import Base
from ..utils.schema_helpers import POST_SCHEMA, utcnow

POSTS_TABLE = f"{POST_SCHEMA}.posts"
COMMENTS_TABLE = f"{POST_SCHEMA}.comments"


class Comment(Base):
    __tablename__ = "comments"
    __table_args__ = {"schema": POST_SCHEMA}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    post_id = Column(UUID(as_uuid=True), ForeignKey(f"{POSTS_TABLE}.id", ondelete="CASCADE"), nullable=False)
    parent_comment_id = Column(UUID(as_uuid=True), ForeignKey(f"{COMMENTS_TABLE}.id", ondelete="CASCADE"))
    user_id = Column(UUID(as_uuid=True), nullable=False)
    content = Column(Text)
    is_anonymous = Column(Boolean, nullable=False, default=False)
    like_count = Column(Integer, nullable=False, default=0)
    reply_count = Column(Integer, nullable=False, default=0)
    report_count = Column(Integer, nullable=False, default=0)
    status = Column(String(30), nullable=False, default="ACTIVE")
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)
    deleted_at = Column(DateTime(timezone=True))

    post = relationship("Post", foreign_keys=[post_id], back_populates="comments")
    parent = relationship(
        "Comment",
        remote_side=[id],
        backref=backref("replies", cascade="all, delete-orphan"),
        foreign_keys=[parent_comment_id],
    )
    likes = relationship("CommentLike", back_populates="comment", cascade="all, delete-orphan")
