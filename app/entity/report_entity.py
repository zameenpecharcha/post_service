from sqlalchemy import Column, DateTime, String, Text
from sqlalchemy.dialects.postgresql import UUID
from uuid import uuid4

from ..utils.db_connection import Base
from ..utils.schema_helpers import MEDIA_SCHEMA, utcnow

REPORT_SCHEMA = MEDIA_SCHEMA  # api_gateway


class Report(Base):
    __tablename__ = "reports"
    __table_args__ = {"schema": REPORT_SCHEMA}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    report_code = Column(String(30), nullable=False, unique=True)
    entity_type = Column(String(30), nullable=False)
    entity_id = Column(UUID(as_uuid=True), nullable=False)
    reported_by = Column(UUID(as_uuid=True), nullable=False)
    reported_user_id = Column(UUID(as_uuid=True))
    reason_code = Column(String(50))
    description = Column(Text)
    status = Column(String(30), nullable=False, default="PENDING")
    priority = Column(String(20), nullable=False, default="MEDIUM")
    reviewed_by = Column(UUID(as_uuid=True))
    reviewed_at = Column(DateTime(timezone=True))
    action_taken = Column(String(50))
    action_note = Column(Text)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)
