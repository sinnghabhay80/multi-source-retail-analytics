CREATE TABLE IF NOT EXISTS hive_iceberg.bronze.returns (
    return_id STRING,
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    return_date TIMESTAMP,
    return_reason STRING,
    return_value DOUBLE,
    _ingestion_time TIMESTAMP
) USING iceberg
PARTITIONED BY (months(return_date));