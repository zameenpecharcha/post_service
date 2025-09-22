from sqlalchemy import MetaData, Table, Column, BigInteger, String, Text, Integer, TIMESTAMP, text
from ..utils.db_connection import Base


# Define shared 'media' table schema locally for this service
media = Table(
    'media',
    Base.metadata,
    Column('id', BigInteger, primary_key=True, nullable=False),
    Column('context_id', BigInteger),
    Column('context_type', String(255)),
    Column('media_type', String(255)),
    Column('media_url', Text),
    Column('media_order', Integer),
    Column('media_size', BigInteger),
    Column('caption', Text),
    Column('uploaded_at', TIMESTAMP, server_default=text('CURRENT_TIMESTAMP')),
)


