import pandas as pd
from utils.logger import get_logger
from utils.db import get_db_engine, copy_to_sql
from sqlalchemy import text

logger = get_logger(__name__)
engine = get_db_engine()


SQL_BY_CITY = """
    SELECT
        city,
        COUNT(DISTINCT order_id)       AS total_orders,
        SUM(quantity)         AS total_quantity,
        SUM(total_amount)     AS total_revenue
    FROM silver.orders_cleaned
    GROUP BY city
    ORDER BY total_revenue DESC;
"""

SQL_BY_PRODUCT = """
    SELECT
        product_name,
        COUNT(DISTINCT order_id)          AS total_orders,
        SUM(quantity)            AS total_quantity,
        SUM(total_amount)        AS total_revenue,
        ROUND(AVG(unit_price_dzd), 2) AS avg_price
    FROM silver.orders_cleaned
    GROUP BY product_name
    ORDER BY total_revenue DESC;
"""



SQL_BY_CUSTOMER = """
    SELECT
        customer_name,
        COUNT(DISTINCT order_id)   AS total_orders,
        SUM(total_amount) AS total_spent
    FROM silver.orders_cleaned
    GROUP BY customer_name
    ORDER BY total_spent DESC;
"""

SQL_BY_STATUS = """
    SELECT
        status,
        COUNT(DISTINCT order_id)   AS total_orders,
        SUM(total_amount) AS total_revenue
    FROM silver.orders_cleaned
    GROUP BY status
    ORDER BY total_orders DESC;
"""

def transform_silver_to_gold():
    queries = {
        "sales_by_city":     SQL_BY_CITY,
        "sales_by_product":  SQL_BY_PRODUCT,
        "sales_by_customer": SQL_BY_CUSTOMER,
        "sales_by_status":   SQL_BY_STATUS,
    }
    try:
        with engine.connect() as conn:         
            for table_name, sql in queries.items(): 
                logger.info("Transforming %s", table_name)
                df = pd.read_sql(text(sql), con=conn)
                copy_to_sql(df, table_name, "gold", engine)
                logger.info("Saved %s rows to gold.%s", len(df), table_name)
    except Exception as e:
        logger.error("Error occurred: %s", e)
        raise RuntimeError("Failed to transform silver to gold") from e
    
def main():
    logger.info("Starting transformation from silver to gold")
    transform_silver_to_gold()
    logger.info("Completed transformation from silver to gold")

if __name__ == "__main__":
    main()