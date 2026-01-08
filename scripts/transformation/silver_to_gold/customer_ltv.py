from utils.spark import get_spark_session
from utils.logger import get_logger
from utils.iceberg import create_iceberg_table, write_iceberg_table
from pyspark.sql.functions import col, round, current_timestamp, sum, countDistinct, avg, min, max

logger = get_logger("GoldCustomeLTVAggregation")

def main():
    spark = get_spark_session()
    source_table = "hive_iceberg.silver.sales"
    target_table = "hive_iceberg.gold.customer_ltv"
    ddl_path = "sql/schema/gold/customer_ltv.sql"

    logger.info(f"Aggregating {source_table} → {target_table}...")
    create_iceberg_table(ddl_path, spark)

    sales = spark.table(source_table)
    customer_ltv = sales \
                    .groupBy(col("customer_id")) \
                    .agg(
                        sum(col("total_amount")).alias("lifetime_value"),
                        countDistinct(col("order_id")).alias("total_orders"),
                        round(avg(col("total_amount")), 2).alias("avg_order_value"),
                        min(col("order_date")).alias("first_order_date"),
                        max(col("order_date")).alias("last_order_date"),
                    ) \
                    .withColumn("_ingestion_time", current_timestamp())

    write_iceberg_table(customer_ltv, target_table, mode="overwrite")
    logger.info(f"Gold table {target_table} updated → {customer_ltv.count():,} customers")


if __name__ == "__main__":
    main()