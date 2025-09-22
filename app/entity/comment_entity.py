from sqlalchemy import Column, BigInteger, String, TIMESTAMP, ForeignKey
from sqlalchemy.orm import relationship, backref
from ..utils.db_connection import Base
from datetime import datetime

class Comment(Base):
    __tablename__ = "comments"
    __table_args__ = {'extend_existing': True}

    id = Column(BigInteger, primary_key=True)
    post_id = Column(BigInteger, ForeignKey('posts.id', ondelete='CASCADE'), nullable=False)
    parent_comment_id = Column(BigInteger, ForeignKey('comments.id', ondelete='CASCADE'), nullable=True)  # NULL for top-level comments
    comment = Column(String(1000))
    user_id = Column(BigInteger, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    status = Column(String(20), default='active')
    added_at = Column(TIMESTAMP, default=datetime.utcnow)
    commented_at = Column(TIMESTAMP, default=datetime.utcnow)

    # Relationships
    user = relationship("User", back_populates="comments")
    post = relationship("Post", foreign_keys=[post_id], back_populates="comments")
    parent = relationship(
        "Comment",
        remote_side=[id],
        backref=backref("replies", cascade="all, delete-orphan"),
        foreign_keys=[parent_comment_id]
    )
    likes = relationship("CommentLike", back_populates="comment", cascade="all, delete-orphan") 