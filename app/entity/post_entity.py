from sqlalchemy import Column, BigInteger, String, TIMESTAMP, ForeignKey, Text, Numeric, Integer
from sqlalchemy.orm import relationship
from datetime import datetime
from ..utils.db_connection import Base
from .comment_entity import Comment

class Post(Base):
    __tablename__ = "posts"

    id = Column(BigInteger, primary_key=True, index=True)
    user_id = Column(BigInteger, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    content = Column(String(2000))
    title = Column(String(255))
    visibility = Column(String(20))
    property_type = Column(String(50))
    location = Column(String(255))
    map_location = Column(String(100))
    price = Column(Numeric(15,2))
    status = Column(String(20))
    created_at = Column(TIMESTAMP, default=datetime.utcnow)

    # Relationships
    user = relationship("User", back_populates="posts")
    media = relationship("PostMedia", back_populates="post", cascade="all, delete-orphan")
    likes = relationship("PostLike", back_populates="post", cascade="all, delete-orphan")
    comments = relationship("Comment", back_populates="post", cascade="all, delete-orphan")

class PostMedia(Base):
    __tablename__ = "post_media"

    id = Column(BigInteger, primary_key=True)
    post_id = Column(BigInteger, ForeignKey('posts.id', ondelete='CASCADE'), nullable=False)
    media_type = Column(String(50))
    media_url = Column(Text)
    media_order = Column(Integer)
    media_size = Column(BigInteger)
    caption = Column(Text)
    uploaded_at = Column(TIMESTAMP, default=datetime.utcnow)

    # Relationships
    post = relationship("Post", back_populates="media")

class PostLike(Base):
    __tablename__ = "post_likes"

    id = Column(BigInteger, primary_key=True)
    post_id = Column(BigInteger, ForeignKey('posts.id', ondelete='CASCADE'), nullable=False)
    reaction_type = Column(String(20))
    user_id = Column(BigInteger, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    liked_at = Column(TIMESTAMP, default=datetime.utcnow)

    # Relationships
    post = relationship("Post", back_populates="likes")
    user = relationship("User", back_populates="post_likes")

class CommentLike(Base):
    __tablename__ = "post_comment_likes"

    id = Column(BigInteger, primary_key=True)
    comment_id = Column(BigInteger, ForeignKey('comments.id', ondelete='CASCADE'), nullable=False)
    reaction_type = Column(String(20))
    user_id = Column(BigInteger, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    liked_at = Column(TIMESTAMP, default=datetime.utcnow)

    # Relationships
    comment = relationship("Comment", back_populates="likes")
    user = relationship("User", back_populates="comment_likes") 