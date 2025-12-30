CREATE TABLE IF NOT EXISTS hive_iceberg.gold.promo_revenue (
    order_date DATE,
    promo_id STRING,
    promo_name STRING,
    revenue_with_promo DOUBLE,
    revenue_without_promo DOUBLE,
    uplift_pct DOUBLE,
    _ingestion_time TIMESTAMP
) USING iceberg
PARTITIONED BY (months(order_date));