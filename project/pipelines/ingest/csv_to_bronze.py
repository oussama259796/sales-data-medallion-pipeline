import pandas as pd
from glob import glob
from pathlib import Path
from utils.logger import get_logger
from utils.db import get_db_engine, copy_to_sql
from sqlalchemy import text

logger = get_logger(__name__)
engine = get_db_engine()

TARGET_COLUMNS = [
    "source_file", "load_timestamp", "order_id",
    "customer_name", "city", "product_name",
    "quantity", "unit_price_dzd", "total_amount", "status",
]

COLUMN_MAP = {
    "prod_name": "product_name",
    "product":   "product_name",
    "prod":      "product_name",
    "cust_name": "customer_name",
}

def normalize_file(file_path):
    df = pd.read_csv(file_path)
    df.columns = df.columns.str.strip()
    df = df.rename(columns=COLUMN_MAP)
    df["source_file"]    = Path(file_path).name
    df["load_timestamp"] = pd.Timestamp.now()
    for col in TARGET_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    df = df.reindex(columns=TARGET_COLUMNS)
    logger.info("Normalized %s → %s rows", Path(file_path).name, len(df))
    return df

# ✅ كل الكود داخل دالة
def main():
    logger.info("Starting ingestion from CSV to bronze")

    files = glob("data/raw/huge_store_*.csv")
    if not files:
        raise FileNotFoundError("No CSV files found in data/raw/")

    bronze_tables = [normalize_file(f) for f in files]
    bronze_union  = pd.concat(bronze_tables, ignore_index=True)

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE bronze.orders_raw;"))

    copy_to_sql(bronze_union, "orders_raw", "bronze", engine)
    logger.info("Loaded %s rows into bronze.orders_raw", len(bronze_union))
    logger.info("Finished ingestion from CSV to bronze")

if __name__ == "__main__":
    main()