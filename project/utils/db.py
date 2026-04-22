from sqlalchemy import create_engine
import os
import io
from dotenv import load_dotenv
from utils.logger import get_logger

logger = get_logger(__name__)
load_dotenv()

def get_db_engine():
    """Create a SQLAlchemy engine using environment variables."""
    try:
        host     = os.getenv('PG_HOST')
        port     = os.getenv('PG_PORT')
        db       = os.getenv('PG_DB')
        user     = os.getenv('PG_USER')
        password = os.getenv('PG_PASSWORD')

        if not all([host, port, db, user, password]):
            raise ValueError("Database connection parameters are not fully set.")

        connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
        engine = create_engine(connection_string)
        logger.info("Database engine created successfully")
        return engine
    except Exception as e:
        logger.error(f"Error creating database engine: {e}")
        raise


def copy_to_sql(df, table_name, schema, engine):
   
    try:
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False, na_rep="")
        buffer.seek(0)

        columns = ", ".join([f'"{c}"' for c in df.columns])

        with engine.begin() as conn:
            raw = conn.connection
            with raw.cursor() as cur:
                cur.copy_expert(
                    f"COPY {schema}.{table_name} ({columns}) FROM STDIN WITH (FORMAT CSV, NULL '')",
                    buffer
                )
        logger.info("copy_to_sql: loaded data into %s.%s", schema, table_name)
    except Exception as e:
        logger.error("copy_to_sql failed for %s.%s: %s", schema, table_name, e)
        raise


