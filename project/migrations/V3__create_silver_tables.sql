CREATE TABLE silver.orders_cleaned (
    source_file VARCHAR(255),
    load_timestamp TIMESTAMP,
    order_id VARCHAR(50) primary key not null,
    customer_name VARCHAR(100),
    city VARCHAR(100),
    product_name VARCHAR(255),
    quantity INTEGER,
    unit_price_dzd DECIMAL(10, 2),
    total_amount DECIMAL(10, 2),
    status VARCHAR(50)
);

cd project; py -m pipelines.transform.bronze_to_silver


select * from silver.orders_cleaned;