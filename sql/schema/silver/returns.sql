CREATE TABLE IF NOT EXISTS hive_iceberg.silver.returns (
    return_id STRING,
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    return_date DATE,
    return_reason STRING,
    return_value DOUBLE,
    is_valid BOOLEAN,
    _ingestion_time TIMESTAMP
) USING iceberg
PARTITIONED BY (months(return_date));