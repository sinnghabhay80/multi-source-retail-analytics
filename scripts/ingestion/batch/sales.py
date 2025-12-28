from utils.spark import get_spark_session
from utils.logger import get_logger
from utils.config import get_project_root
from utils.iceberg import create_iceberg_table, write_iceberg_table

from pyspark.sql.functions import current_timestamp, col, to_date

logger = get_logger(__name__)


def main():
    spark = get_spark_session()
    data_path = "data/raw/sales"
    project_root = get_project_root()
    full_path = (project_root / data_path).resolve()
    ddl_path = "sql/schema/bronze/sales.sql"
    table_name = "hive_iceberg.bronze.sales"
    logger.info(f"Ingesting raw sales → {table_name}...")
    create_iceberg_table(ddl_path, spark)

    bronze_sales_df = spark\
                        .read\
                        .format("csv")\
                        .option("header", "true")\
                        .option("inferSchema", "true")\
                        .load(str(full_path))\
                        .withColumn("_ingestion_time", current_timestamp())

    write_iceberg_table(bronze_sales_df, table_name)
    logger.info(f"Bronze Table: {table_name} loaded → {bronze_sales_df.count():,} rows.")

if __name__ == "__main__":
    main()