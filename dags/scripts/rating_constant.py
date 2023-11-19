from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType

SPARK_SESSION = (
    SparkSession.builder.config(
        "spark.jars.packages", "org.postgresql:postgresql:42.5.4"
    )
    .master("local[*]")
    .getOrCreate()
)

SCHEMA = StructType(
    [
        StructField("item_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("rating", FloatType(), True),
        StructField("timestamp", LongType(), True),
    ]
)

URL_POSTGRES = "jdbc:postgresql://10.0.1.5:5432/postgres"
COLUMN_NAME = "review_date"
TABLE_NAME = "avg_rating_per_month"
PROPERTIES = {"user": "postgres", "password": "1234", "driver": "org.postgresql.Driver"}
