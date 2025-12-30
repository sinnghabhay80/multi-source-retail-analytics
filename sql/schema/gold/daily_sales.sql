CREATE TABLE IF NOT EXISTS hive_iceberg.gold.daily_sales (
    order_date DATE,
    total_revenue DOUBLE,
    total_orders BIGINT,
    avg_order_value DOUBLE,
    _ingestion_time TIMESTAMP
) USING iceberg
PARTITIONED BY (months(order_date));