from utils.spark import get_spark_session
from utils.logger import get_logger
from utils.iceberg import create_iceberg_table, write_iceberg_table
from pyspark.sql.functions import col, round, to_date, lit, current_timestamp

logger = get_logger("SilverSalesIngestion")

def main():
    spark = get_spark_session()
    source_table = "hive_iceberg.bronze.sales"
    target_table = "hive_iceberg.silver.sales"
    ddl_path = "sql/schema/silver/sales.sql"

    logger.info(f"Transforming {source_table} → {target_table}...")
    create_iceberg_table(ddl_path, spark)

    bronze = spark.table(source_table)
    silver = bronze.filter((col("quantity") > 0) & (col("price") > 0)) \
                    .withColumn("total_amount", round(col("quantity") * col("price"), 2)) \
                    .withColumn("order_date", to_date("order_date"))  \
                    .withColumn("is_valid", lit(True)) \
                    .withColumn("_ingestion_time", current_timestamp()) \
                    .select(
                        "order_id", "customer_id", "product_id", "quantity", "price",
                        "total_amount", "order_date", "is_valid", "_ingestion_time"
                    )

    write_iceberg_table(silver, target_table)
    logger.info(f"Silver table {target_table} created → {silver.count():,} valid rows")


if __name__ == "__main__":
    main()