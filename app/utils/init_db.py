"""
Database bootstrap is managed by api_gateway/create_tables_updated.sql.
This module only verifies connectivity — it does not create or drop tables.
"""
import logging

from .db_connection import get_db_engine
from sqlalchemy import text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def init_database() -> bool:
    engine = get_db_engine()
    with engine.connect() as conn:
        conn.execute(text('SELECT 1 FROM "post".posts LIMIT 0'))
        conn.commit()
    logger.info('Verified post schema tables exist (run create_tables_updated.sql if missing).')
    return True


if __name__ == "__main__":
    init_database()
