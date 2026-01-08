from pyspark.sql import SparkSession
from utils.config import load_config
from utils.logger import get_logger

logger = get_logger("SparkUtils")

def get_spark_session(config_path: str = "configs/spark/spark-config.yaml"):
    config = load_config(config_path)["spark"]

    spark = SparkSession.builder\
            .appName(config['app_name'])\
            .master(config['master'])\
            .config("spark.sql.catalog.hive_iceberg", "org.apache.iceberg.spark.SparkCatalog")\
            .config("spark.sql.catalog.hive_iceberg.type", "hive")\
            .config("spark.sql.catalog.hive_iceberg.uri", config['iceberg']['hive_metastore_uri'])\
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
            .config("spark.hadoop.fs.s3a.endpoint", config['s3']['endpoint'])\
            .config("spark.hadoop.fs.s3a.access.key", config['s3']['access_key'])\
            .config("spark.hadoop.fs.s3a.secret.key", config['s3']['secret_key'])\
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.sql.iceberg.compression-codec", "snappy") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.iceberg.write.parquet.row-group-size-bytes", 32 * 1024 * 1024) \
            .config("spark.sql.parquet.enableVectorizedWriter", "false") \
            .config("spark.sql.shuffle.partitions", "16") \
            .config("spark.sql.iceberg.write.distribution-mode","none") \
            .getOrCreate()

    logger.info(f"Spark session: {spark.version} started...")

    return spark
