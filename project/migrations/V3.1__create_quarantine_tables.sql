CREATE TABLE IF NOT EXISTS silver.orders_rejected (
    source_file VARCHAR(255),
    load_timestamp TIMESTAMP,
    order_id VARCHAR(50),
    customer_name VARCHAR(100),
    city VARCHAR(100),
    product_name VARCHAR(255),
    quantity INTEGER,
    unit_price_dzd DECIMAL(10, 2),
    total_amount DECIMAL(10, 2),
    rejection_reason VARCHAR(255),
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50));

SELECT * FROM silver.orders_rejected;