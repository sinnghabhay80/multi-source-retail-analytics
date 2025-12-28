CREATE TABLE IF NOT EXISTS hive_iceberg.bronze.promotions (
    promo_id STRING,
    promo_name STRING,
    discount_pct DOUBLE,
    start_date DATE,
    end_date DATE,
    target_segment STRING,
    status STRING,
    _ingestion_time TIMESTAMP
) USING iceberg
PARTITIONED BY (years(start_date));