CREATE TABLE IF NOT EXISTS hive_iceberg.bronze.sales (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    quantity DOUBLE,
    price DOUBLE,
    order_date TIMESTAMP,
    _ingestion_time TIMESTAMP
) USING iceberg
PARTITIONED BY (months(order_date))
