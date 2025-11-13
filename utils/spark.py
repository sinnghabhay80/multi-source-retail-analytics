from pyspark.sql import SparkSession
from utils.config import load_config

def get_spark_session(config_path: str = "configs/spark/spark-config.yaml"):
    config = load_config(config_path)["spark"]

    spark = SparkSession.builder\
            .appName(config['app_name'])\
            .master(config['master'])\
            .config("spark.sql.catalog.hive", "org.apache.iceberg.spark.SparkCatalog")\
            .config("spark.sql.catalog.hive.type", "hive")\
            .config("spark.sql.catalog.hive.uri", config['iceberg']['hive_metastore_uri'])\
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
            .config("spark.sql.catalog.hive.warehouse", config['iceberg']['warehouse'])\
            .config("spark.jars.packages", ",".join(config['jars']))\
            .config("spark.hadoop.fs.s3a.endpoint", config['s3']['endpoint'])\
            .config("spark.hadoop.fs.s3a.access.key", config['s3']['access_key'])\
            .config("spark.hadoop.fs.s3a.secret.key", config['s3']['secret_key'])\
            .config("spark.hadoop.fs.s3a.path.style.access", "true")\
            .getOrCreate()

    return spark