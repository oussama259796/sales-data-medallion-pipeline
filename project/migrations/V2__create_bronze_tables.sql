CREATE TABLE bronze.orders_raw (
    source_file VARCHAR(255),
    load_timestamp TIMESTAMP,
    order_id VARCHAR(50),
    customer_name VARCHAR(100),
    city VARCHAR(100),
    product_name VARCHAR(255),
    quantity INT(50),
    unit_price_dzd DECIMAL(10, 2),
    total_amount DECIMAL(10, 2),
    status VARCHAR(50)
);
SELECT * FROM bronze.orders_raw;