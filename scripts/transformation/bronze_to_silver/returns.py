from utils.spark import get_spark_session
from utils.logger import get_logger
from utils.iceberg import create_iceberg_table, write_iceberg_table
from pyspark.sql.functions import col, round, to_date, lit, when

logger = get_logger(__name__)

def main():
    spark = get_spark_session()
    source_table = "hive_iceberg.bronze.returns"
    target_table = "hive_iceberg.silver.returns"
    sales_silver_table = "hive_iceberg.silver.sales"
    ddl_path = "sql/schema/silver/returns.sql"

    logger.info(f"Transforming {source_table} → {target_table}...")
    create_iceberg_table(ddl_path, spark)

    bronze = spark.table(source_table)
    sales_order_ids = spark.table(sales_silver_table).select("order_id").distinct()
    silver = bronze.alias("returns").filter(col("return_value") > 0) \
                    .filter(col("returns.order_id").isNotNull()) \
                    .withColumn("return_date", to_date(col("returns.return_date"))) \
                    .join(sales_order_ids.alias("sales"), "order_id", "left") \
                    .withColumn("is_valid", when(col("returns.order_id").isNotNull() &
                                                 col("sales.order_id").isNotNull() &
                                                 col("return_reason").isin("Defective", "Wrong size", "Not as described"), True)
                                            .otherwise(False)) \
                    .select(
                        "return_id", "order_id", "customer_id", "product_id",
                        "return_date", "return_reason", "return_value", "is_valid", "_ingestion_time"
                    )


    write_iceberg_table(silver, target_table)
    logger.info(f"Silver table {target_table} created → {silver.count():,} valid rows")


if __name__ == "__main__":
    main()