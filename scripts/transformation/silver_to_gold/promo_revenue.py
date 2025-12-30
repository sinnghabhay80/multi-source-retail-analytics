from utils.spark import get_spark_session
from utils.logger import get_logger
from utils.iceberg import create_iceberg_table, write_iceberg_table
from pyspark.sql.functions import col, current_timestamp, sum, avg, when, round, lit, row_number, explode, sequence

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

    promo_days = promos \
                  .select(explode(sequence("start_date", "end_date")).alias("promo_date")) \
                  .select("promo_date") \
                  .distinct()

    daily_sales = sales \
                   .groupBy("order_date") \
                   .agg(sum("total_amount").alias("daily_revenue"))

    non_promo_daily = daily_sales \
                        .join(promo_days, daily_sales.order_date == promo_days.promo_date, "left_anti") \
                        .agg(avg("daily_revenue").alias("avg_non_promo_daily")) \
                        .collect()[0]["avg_non_promo_daily"]

    promo_revenue = daily_sales \
                        .join(promo_days, daily_sales.order_date == promo_days.promo_date, "inner") \
                        .withColumn("revenue_with_promo", col("daily_revenue")) \
                        .withColumn("revenue_without_promo", lit(non_promo_daily)) \
                        .withColumn("uplift_pct",
                                    when(lit(non_promo_daily) > 0,
                                           round((col("daily_revenue") - lit(non_promo_daily)) / lit(non_promo_daily) * 100, 2))
                                    .otherwise(0.0)
                                    ) \
                        .select(
                            col("order_date"),
                            lit("Multiple/Overlapping").alias("promo_id"),
                            lit("Active Promos").alias("promo_name"),
                            "revenue_with_promo",
                            "revenue_without_promo",
                            "uplift_pct",
                            current_timestamp().alias("_ingestion_time")
                        )

    write_iceberg_table(promo_revenue, target_table, mode="overwrite")
    logger.info(f"Gold table {target_table} updated → {promo_revenue.count():,} rows")


if __name__ == "__main__":
    main()