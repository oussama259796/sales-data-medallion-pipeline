import pandas as pd
from utils.logger import get_logger
from utils.db import get_db_engine, copy_to_sql   # ← أضفنا copy_to_sql
from sqlalchemy import text

logger = get_logger(__name__)
engine = get_db_engine()

number_columns = ["quantity", "unit_price_dzd", "total_amount", "discount_pct"]
text_columns   = ["source_file", "customer_name", "city", "product_name", "status"]


def transform_bronze_to_silver():
    # ── 1. قراءة Bronze ──────────────────────────────────────────
    try:
        df = pd.read_sql("SELECT * FROM bronze.orders_raw", con=engine)
        logger.info("Read %s rows from bronze.orders_raw", len(df))
    except Exception as e:
        raise RuntimeError("Failed to read from bronze.orders_raw") from e

    if df.empty:
        raise ValueError("No data to transform from bronze.orders_raw")

    df = df.copy()

   
   # ── 2. تنظيف الأعمدة ─────────────────────────────────────
    for col in number_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["quantity"] = df["quantity"].astype("Int64")

    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].fillna("unknown").astype(str).str.strip().str.title()

    if "order_date" in df.columns:
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

# ── 3. تحديد المرفوضين ───────────────────────────────────
    df["rejection_reason"] = None

# ✅ تحقق من NaN قبل fillna
    null_mask = (
    df["quantity"].isna() |
    df["unit_price_dzd"].isna() |
    df["total_amount"].isna()
)
    df.loc[null_mask, "rejection_reason"] = "Missing required fields"

# ثم املأ الـ NaN
    df["quantity"]       = df["quantity"].fillna(0)
    df["unit_price_dzd"] = df["unit_price_dzd"].fillna(0)
    df["total_amount"]   = df["total_amount"].fillna(0)

# ثم تحقق من القيم السالبة فقط
    neg_mask = (
    df["rejection_reason"].isna() & (
        (df["quantity"] < 0) |
        (df["unit_price_dzd"] <= 0) |
        (df["total_amount"] <= 0)
    )
)
    df.loc[neg_mask, "rejection_reason"] = "Invalid values"

    rejected_df = df[df["rejection_reason"].notna()].copy()
    valid_df    = df[df["rejection_reason"].isna()].copy()

    logger.info("Valid: %s | Rejected: %s", len(valid_df), len(rejected_df))

    # ── 4. ترتيب وإزالة تكرار valid ──────────────────────────────
    if "load_timestamp" in valid_df.columns:
        valid_df = valid_df.sort_values("load_timestamp")

    valid_df = valid_df.drop_duplicates("order_id", keep="last")

    # ── 5. حفظ المرفوضين ─────────────────────────────────────────
    if not rejected_df.empty:
        rejected_df["rejected_at"] = pd.Timestamp.now()
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE silver.orders_rejected;"))
        copy_to_sql(rejected_df, "orders_rejected", "silver", engine)
        logger.info("Inserted %s rows into silver.orders_rejected", len(rejected_df))

    # ── 6. حفظ النظيفين ──────────────────────────────────────────
    clean_df = valid_df.drop(columns=["rejection_reason"], errors="ignore")

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE silver.orders_cleaned;"))

    copy_to_sql(clean_df, "orders_cleaned", "silver", engine)
    logger.info("Inserted %s rows into silver.orders_cleaned", len(clean_df))

    
def main():
        logger.info("Starting transformation from bronze to silver")
        transform_bronze_to_silver()
        logger.info("Finished transformation from bronze to silver")


if __name__ == "__main__":
    main()
    


