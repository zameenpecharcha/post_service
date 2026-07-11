from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import logging
from ..entity.post_entity import Base, Post

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_database():
    # Load environment variables
    load_dotenv()

    # Get database configuration
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_NAME = os.getenv("DB_NAME", "postgres")  # Using postgres as default
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
    DB_PORT = os.getenv("DB_PORT", "5432")

    try:
        # Connect to database
        engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        
        # Drop existing tables with CASCADE
        with engine.connect() as connection:
            # Drop the post_likes table first
            connection.execute(text("DROP TABLE IF EXISTS post_likes CASCADE"))
            connection.execute(text("DROP TABLE IF EXISTS posts CASCADE"))
            connection.commit()
            logger.info("Dropped existing tables")
        
        # Create all tables from scratch
        Base.metadata.create_all(bind=engine)
        logger.info("Created all tables successfully")
        
        return True

    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        raise

if __name__ == "__main__":
    init_database() 