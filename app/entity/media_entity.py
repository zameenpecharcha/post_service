from sqlalchemy import BigInteger, Boolean, Column, DateTime, Integer, SmallInteger, String, Table, Text
from sqlalchemy.dialects.postgresql import UUID

from ..utils.db_connection import Base
from ..utils.schema_helpers import MEDIA_SCHEMA

media = Table(
    "media",
    Base.metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, nullable=False),
    Column("media_code", String(30), nullable=False),
    Column("entity_type", String(30)),
    Column("entity_id", UUID(as_uuid=True)),
    Column("uploaded_by", UUID(as_uuid=True)),
    Column("media_type", String(30)),
    Column("file_name", String(255)),
    Column("file_url", Text),
    Column("thumbnail_url", Text),
    Column("mime_type", String(100)),
    Column("file_size", BigInteger),
    Column("width", Integer),
    Column("height", Integer),
    Column("duration_seconds", Integer),
    Column("display_order", SmallInteger, nullable=False, default=1),
    Column("is_cover", Boolean, nullable=False, default=False),
    Column("storage_provider", String(30)),
    Column("bucket_name", String(100)),
    Column("object_key", String(255)),
    Column("status", String(30)),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True)),
    schema=MEDIA_SCHEMA,
)
