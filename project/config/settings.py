import os
from dotenv import load_dotenv
# Load environment variables from .env file
load_dotenv()

# PostgreSQL Configuration
PG_HOST = os.getenv("PG_HOST")
PG_PORT = int(os.getenv("PG_PORT"))
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")