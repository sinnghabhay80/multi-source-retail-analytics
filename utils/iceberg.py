from pyspark.sql import DataFrame
from utils.spark import get_spark_session
from utils.logger import get_logger

logger = get_logger(__name__)

def read_sql_from_path(path):
    with open(path) as f:
        return f.read()

def create_iceberg_table(sql_path: str):
    spark = get_spark_session()
    sql = read_sql_from_path(sql_path)
    logger.info(f"Creating iceberg table: {sql.split()[5]}")
    spark.sql(sql)
    logger.info(f"Created Iceberg table: {sql.split()[5]}.")

def write_iceberg_table(df: DataFrame, table: str, mode: str = "append"):
    logger.info(f"Writing iceberg table: {table}")
    df.write.format("iceberg").mode(mode).save(table)
    logger.info(f"Wrote iceberg table: {table}.")
