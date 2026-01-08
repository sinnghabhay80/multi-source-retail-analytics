from utils.spark import get_spark_session
from utils.logger import get_logger
from utils.iceberg import create_iceberg_table, write_iceberg_table
from pyspark.sql.functions import col, round, current_timestamp, sum, count, trunc, when, lit

logger = get_logger("GoldReturnRateByProductAggregation")

def main():
    spark = get_spark_session()
    sales_table = "hive_iceberg.silver.sales"
    returns_table = "hive_iceberg.silver.returns"
    target_table = "hive_iceberg.gold.return_rate_by_product"
    ddl_path = "sql/schema/gold/return_rate_by_product.sql"

    logger.info(f"Aggregating {sales_table} + {returns_table} → {target_table}...")
    create_iceberg_table(ddl_path, spark)

    sales_monthly = spark.table(sales_table) \
                         .withColumn("return_month", trunc(col("order_date"), "month")) \
                         .groupBy(col("product_id"), col("return_month")) \
                         .agg(sum(col("quantity")).alias("sold_qty"))

    returns_monthly = spark.table(returns_table) \
                           .withColumn("return_month", trunc(col("return_date"), "month")) \
                           .groupBy(col("product_id"), col("return_month")) \
                           .agg(
                             sum(col("return_value")).alias("total_return_value"),
                             count("*").alias("returned_qty")
                           )

    return_rate_by_product = sales_monthly \
                                .join(returns_monthly, ["product_id", "return_month"], "left") \
                                .fillna(0, subset=["returned_qty", "total_return_value"]) \
                                .withColumn("return_rate",
                                            when(col("total_return_value") != 0, round(col("returned_qty") / col("total_return_value"), 4))
                                            .otherwise(round(lit(0), 4))
                                            ) \
                                .withColumn("_ingestion_time", current_timestamp())

    write_iceberg_table(return_rate_by_product, target_table, mode="overwrite")
    logger.info(f"Gold table {target_table} updated → {return_rate_by_product.count():,} product-month rows")


if __name__ == "__main__":
    main()