from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

SPARK_SESSION = SparkSession.builder.master("local[*]").getOrCreate()

SCHEMA = StructType(
    [
        StructField("_corrupt_record", StringType(), True),
        StructField("asin", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("categories", StringType(), True),
        StructField("description", StringType(), True),
        StructField("imUrl", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("related", StringType(), True),
        StructField("salesRank", StringType(), True),
        StructField("title", StringType(), True),
    ]
)

URL_POSTGRES = "jdbc:postgresql://10.0.1.5:5432/postgres"
TABLE_NAME = "rating_metadata"
PROPERTIES = {"user": "postgres", "password": "1234", "driver": "org.postgresql.Driver"}
