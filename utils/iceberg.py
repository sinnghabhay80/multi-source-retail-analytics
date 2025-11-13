from pyspark.sql import DataFrame
from utils.spark import get_spark_session
from utils.logger import get_logger

logger = get_logger(__name__)

def create_iceberg_table(sql: str):
    spark = get_spark_session()
    logger.info(f"Creating iceberg table: {sql}")
    spark.sql(sql)
    logger.info(f"Created Iceberg table: {sql.split()[5]}.")

def write_iceberg_table(df: DataFrame, table: str, mode: str = "append"):
    logger.info(f"Writing iceberg table: {table}")
    df.write.format("iceberg").mode(mode).save(table)
    logger.info(f"Wrote iceberg table: {table}.")
