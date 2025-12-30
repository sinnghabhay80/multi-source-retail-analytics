from utils.spark import get_spark_session
from utils.logger import get_logger
from utils.iceberg import create_iceberg_table, write_iceberg_table
from pyspark.sql.functions import col, current_timestamp, sum, when, round, lit

logger = get_logger(__name__)

def main():
    spark = get_spark_session()
    sales_table = "hive_iceberg.silver.sales"
    promos_table = "hive_iceberg.silver.promotions"
    target_table = "hive_iceberg.gold.promo_revenue"
    ddl_path = "sql/schema/gold/promo_revenue.sql"

    logger.info(f"Aggregating {sales_table} + {promos_table} → {target_table}...")
    create_iceberg_table(ddl_path, spark)

    sales = spark.table(sales_table)
    promos = spark.table(promos_table)

    joined = sales.alias("sales") \
                .join(promos.alias("promos"),
                      (col("sales.order_date") >= col("promos.start_date")) &
                      (col("sales.order_date") <= col("promos.end_date")),
                      "left"
                      ) \
                .select(
                    col("sales.order_date"),
                    col("promos.promo_id"),
                    col("promos.promo_name"),
                    col("sales.total_amount").alias("revenue_with_promo")
                )

    baseline_sales = sales \
                        .groupBy(col("order_date")) \
                        .agg(sum("total_amount").alias("daily_baseline"))

    promo_revenue = joined \
                        .groupBy(col("order_date"), col("promo_id"), col("promo_name")) \
                        .agg(sum(col("revenue_with_promo")).alias("revenue_with_promo")) \
                        .join(baseline_sales, "order_date", "left") \
                        .withColumn("revenue_without_promo", col("daily_baseline")) \
                        .withColumn("uplift_pct", 
                                    when(col("daily_baseline") > 0,
                                         round((col("revenue_with_promo") - col("daily_baseline")) / col("daily_baseline") * 100, 2))
                                    .otherwise(round(lit(0), 2))
                                    ) \
                        .withColumn("_ingestion_time", current_timestamp()) \
                        .drop(col("daily_baseline"))

    write_iceberg_table(promo_revenue, target_table, mode="overwrite")
    logger.info(f"Gold table {target_table} updated → {promo_revenue.count():,} rows")


if __name__ == "__main__":
    main()