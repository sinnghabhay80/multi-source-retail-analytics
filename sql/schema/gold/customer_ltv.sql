CREATE TABLE IF NOT EXISTS hive_iceberg.gold.customer_ltv (
    customer_id STRING,
    lifetime_value DOUBLE,
    total_orders BIGINT,
    avg_order_value DOUBLE,
    first_order_date DATE,
    last_order_date DATE,
    _ingestion_time TIMESTAMP
) USING iceberg;