CREATE TABLE IF NOT EXISTS hive_iceberg.silver.sales (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    quantity DOUBLE,
    price DOUBLE,
    total_amount DOUBLE,
    order_date DATE,
    is_valid BOOLEAN,
    _ingestion_time TIMESTAMP
) USING iceberg
PARTITIONED BY (months(order_date));