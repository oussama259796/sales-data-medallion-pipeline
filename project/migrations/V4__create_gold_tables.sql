CREATE TABLE gold.sales_by_city (
    city VARCHAR(100),
    total_orders INTEGER,
    total_quantity INTEGER,
    total_revenue DECIMAL(10, 2)
);

CREATE TABLE gold.sales_by_product (
    product_name VARCHAR(255),
    total_orders INTEGER,
    total_quantity INTEGER,
    total_revenue DECIMAL(15, 2),
    avg_price DECIMAL(15, 2)
);

CREATE TABLE gold.sales_by_day (
    order_date DATE,
    total_orders INTEGER,
    total_revenue DECIMAL(15, 2)
);

CREATE TABLE gold.sales_by_customer (
    customer_name VARCHAR(100),
    total_orders INTEGER,
    total_spent DECIMAL(15, 2)
);

CREATE TABLE gold.sales_by_status (
    status VARCHAR(50),
    total_orders INTEGER,
    total_revenue DECIMAL(15, 2)
);

SELECT * FROM gold.sales_by_city;
SELECT * FROM gold.sales_by_product;
SELECT * FROM gold.sales_by_day;
SELECT * FROM gold.sales_by_customer;
SELECT * FROM gold.sales_by_status;


TRUNCATE TABLE gold.sales_by_city;
TRUNCATE TABLE gold.sales_by_product;   
TRUNCATE TABLE gold.sales_by_day;
TRUNCATE TABLE gold.sales_by_customer;
TRUNCATE TABLE gold.sales_by_status;