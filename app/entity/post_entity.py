from sqlalchemy import (
    Boolean, Column, DateTime, ForeignKey, Integer, Numeric, String, Text, BigInteger,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from uuid import uuid4

from ..utils.db_connection import Base
from ..utils.schema_helpers import POST_SCHEMA, utcnow

POSTS_TABLE = f"{POST_SCHEMA}.posts"


class Post(Base):
    __tablename__ = "posts"
    __table_args__ = {"schema": POST_SCHEMA}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    post_code = Column(String(30), nullable=False, unique=True)
    user_id = Column(UUID(as_uuid=True), nullable=False)
    title = Column(String(255))
    content = Column(Text)
    post_type = Column(String(30), nullable=False, default="TEXT")
    property_id = Column(UUID(as_uuid=True))
    visibility = Column(String(30), nullable=False, default="PUBLIC")
    location = Column(String(255))
    latitude = Column(Numeric(10, 7))
    longitude = Column(Numeric(10, 7))
    price = Column(Numeric(18, 2))
    currency = Column(String(10), default="INR")
    is_anonymous = Column(Boolean, nullable=False, default=False)
    allow_comments = Column(Boolean, nullable=False, default=True)
    allow_share = Column(Boolean, nullable=False, default=True)
    allow_reactions = Column(Boolean, nullable=False, default=True)
    status = Column(String(30), nullable=False, default="DRAFT")
    like_count = Column(Integer, nullable=False, default=0)
    comment_count = Column(Integer, nullable=False, default=0)
    share_count = Column(Integer, nullable=False, default=0)
    view_count = Column(BigInteger, nullable=False, default=0)
    report_count = Column(Integer, nullable=False, default=0)
    is_pinned = Column(Boolean, nullable=False, default=False)
    pinned_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)
    deleted_at = Column(DateTime(timezone=True))

    likes = relationship("PostLike", back_populates="post", cascade="all, delete-orphan")
    comments = relationship("Comment", back_populates="post", cascade="all, delete-orphan")


class PostLike(Base):
    __tablename__ = "post_likes"
    __table_args__ = {"schema": POST_SCHEMA}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    post_id = Column(UUID(as_uuid=True), ForeignKey(f"{POSTS_TABLE}.id", ondelete="CASCADE"), nullable=False)
    user_id = Column(UUID(as_uuid=True), nullable=False)
    reaction_type = Column(String(20), nullable=False, default="LIKE")
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)

    post = relationship("Post", back_populates="likes")


class CommentLike(Base):
    __tablename__ = "comment_likes"
    __table_args__ = {"schema": POST_SCHEMA}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    comment_id = Column(UUID(as_uuid=True), ForeignKey(f"{POST_SCHEMA}.comments.id", ondelete="CASCADE"), nullable=False)
    user_id = Column(UUID(as_uuid=True), nullable=False)
    reaction_type = Column(String(20), nullable=False, default="LIKE")
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)

    comment = relationship("Comment", back_populates="likes")


class PostShare(Base):
    __tablename__ = "post_shares"
    __table_args__ = {"schema": POST_SCHEMA}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    share_code = Column(String(30), nullable=False, unique=True)
    post_id = Column(UUID(as_uuid=True), ForeignKey(f"{POSTS_TABLE}.id", ondelete="CASCADE"), nullable=False)
    shared_by = Column(UUID(as_uuid=True), nullable=False)
    share_type = Column(String(20), nullable=False, default="SHARE")
    caption = Column(Text)
    visibility = Column(String(30), nullable=False, default="PUBLIC")
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
