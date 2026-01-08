from utils.spark import get_spark_session
from utils.logger import get_logger
from utils.iceberg import create_iceberg_table, write_iceberg_table
from pyspark.sql.functions import col, round, current_timestamp, sum, countDistinct, avg

logger = get_logger("GoldDailySalesAggregation")

def main():
    spark = get_spark_session()
    source_table = "hive_iceberg.silver.sales"
    target_table = "hive_iceberg.gold.daily_sales"
    ddl_path = "sql/schema/gold/daily_sales.sql"

    logger.info(f"Aggregating {source_table} → {target_table}...")
    create_iceberg_table(ddl_path, spark)

    sales = spark.table(source_table)
    daily_sales = sales.groupBy(col("order_date")) \
                    .agg(
                        sum(col("total_amount")).alias("total_revenue"),
                        countDistinct("order_id").alias("total_orders"),
                        round(avg(col("total_amount")), 2).alias("avg_order_value"),
                    ) \
                    .withColumn("_ingestion_time", current_timestamp())

    write_iceberg_table(daily_sales, target_table, mode="overwrite")
    logger.info(f"Gold table {target_table} updated → {daily_sales.count():,} daily rows")


if __name__ == "__main__":
    main()