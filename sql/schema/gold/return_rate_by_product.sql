CREATE TABLE IF NOT EXISTS hive_iceberg.gold.return_rate_by_product (
    product_id STRING,
    return_month DATE,
    sold_qty BIGINT,
    returned_qty BIGINT,
    return_rate DOUBLE,
    total_return_value DOUBLE,
    _ingestion_time TIMESTAMP
) USING iceberg
PARTITIONED BY (months(return_month));