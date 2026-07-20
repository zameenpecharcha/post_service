from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv
import os

Base = declarative_base()


def _db_setting(primary: str, fallback: str, default: str | None = None) -> str | None:
    return os.getenv(primary) or os.getenv(fallback) or default


def get_db_engine():
    load_dotenv()

    db_user = _db_setting("DB_USER", "POSTGRES_USER")
    db_password = _db_setting("DB_PASSWORD", "POSTGRES_PASSWORD")
    db_host = _db_setting("DB_HOST", "POSTGRES_HOST")
    db_port = _db_setting("DB_PORT", "POSTGRES_PORT", "5432")
    db_name = _db_setting("DB_NAME", "POSTGRES_DB", "postgres")
    ssl_mode = _db_setting("DB_SSLMODE", "POSTGRES_SSLMODE", "")

    print("Attempting to connect to database with:")
    print(f"User: {db_user}")
    print(f"Host: {db_host}")
    print(f"Port: {db_port}")
    print(f"Database: {db_name}")

    if not all([db_user, db_password, db_host, db_name]):
        raise ValueError(
            "Missing database configuration. Set DB_* or POSTGRES_* in your .env file."
        )

    database_url = (
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )
    connect_args = {"sslmode": ssl_mode} if ssl_mode else {}
    engine = create_engine(
        database_url,
        connect_args=connect_args,
        pool_pre_ping=True,
        pool_recycle=280,  # Neon closes idle SSL well under 5 min
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        echo=False,
    )

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            conn.commit()
        print("Connected to PostgreSQL database successfully!")
    except Exception as conn_err:
        print(f"[WARN] DB connection test failed (will retry on first request): {conn_err}")

    return engine
