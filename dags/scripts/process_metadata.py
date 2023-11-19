import sys
from postgres_service import PostgresService
from spark_service import SparkService
import metadata_constant as c

if __name__ == "__main__":
    spark_service = SparkService(c.SPARK_SESSION)
    df = spark_service.get_dataframe_from_file(sys.argv[1], sys.argv[2], c.SCHEMA)

    spark_service = PostgresService(c.SPARK_SESSION)
    spark_service.save_dataframe_to_table(df, c.TABLE_NAME)
