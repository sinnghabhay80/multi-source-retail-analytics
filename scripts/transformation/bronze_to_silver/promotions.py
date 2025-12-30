from utils.spark import get_spark_session
from utils.logger import get_logger
from utils.iceberg import create_iceberg_table, write_iceberg_table
from pyspark.sql.functions import col, to_date, datediff, when

logger = get_logger(__name__)

def main():
    spark = get_spark_session()
    source_table = "hive_iceberg.bronze.promotions"
    target_table = "hive_iceberg.silver.promotions"
    ddl_path = "sql/schema/silver/promotions.sql"

    logger.info(f"Transforming {source_table} → {target_table}...")
    create_iceberg_table(ddl_path, spark)

    bronze = spark.table(source_table)
    silver = bronze.withColumn("start_date", to_date("start_date")) \
                    .withColumn("end_date", to_date("end_date")) \
                    .withColumn("duration_days", datediff("end_date", "start_date")) \
                    .withColumn("is_valid",
                                when((col("start_date") < col("end_date")) &
                                       (col("discount_pct").between(5, 50)), True)
                                 .otherwise(False)) \
                    .select(
                        "promo_id", "promo_name", "discount_pct", "start_date", "end_date",
                        "target_segment", "status", "duration_days", "is_valid", "_ingestion_time"
                    )

    write_iceberg_table(silver, target_table)
    logger.info(f"Silver table {target_table} created → {silver.count():,} valid rows")


if __name__ == "__main__":
    main()