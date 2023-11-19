import sys
from postgres_service import PostgresService
from spark_service import SparkService
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import to_date, avg, year, month, coalesce, count
import rating_constant as c
from pyspark.storagelevel import StorageLevel

if __name__ == "__main__":
    spark_service = SparkService(c.SPARK_SESSION)
    spark_postgres_service = PostgresService(c.SPARK_SESSION)
    df = spark_service.get_dataframe_from_file(sys.argv[1], sys.argv[2], c.SCHEMA)

    df_trfm = df.withColumn(
        c.COLUMN_NAME, to_date(df["timestamp"].cast(TimestampType()), "yyyy-MM-dd")
    )
    df_avg = df_trfm.groupby(
        "item_id",
        year(df_trfm["review_date"]).alias("year"),
        month(df_trfm["review_date"]).alias("month"),
    ).agg(avg("rating").alias("avg_rating"), count("item_id").alias("ratings_count"))
    table_exist = spark_postgres_service.check_table_exists(c.TABLE_NAME)

    if not table_exist:
        df_to_save = df_avg
    else:
        df_postgresql = spark_postgres_service.get_dataframe_from_table(c.TABLE_NAME)

        df_upd = df_avg.join(
            df_postgresql, ["item_id", "year", "month"], how="inner"
        ).select(
            df_avg["item_id"],
            df_avg["year"],
            df_avg["month"],
            coalesce(df_avg["avg_rating"], df_postgresql["avg_rating"]).alias(
                "avg_rating"
            ),
        )
        df_new_rating = (
            df_avg.join(df_postgresql, ["item_id", "year", "month"], how="left")
            .select(
                df_avg["item_id"], df_avg["year"], df_avg["month"], df_avg["avg_rating"]
            )
            .filter(df_postgresql["item_id"].isNull())
        )
        df_old_rating = (
            df_postgresql.join(df_avg, ["item_id", "year", "month"], how="left")
            .select(
                df_postgresql["item_id"],
                df_postgresql["year"],
                df_postgresql["month"],
                df_postgresql["avg_rating"],
            )
            .filter(df_avg["item_id"].isNull())
        )
        df_union = df_old_rating.union(df_new_rating).union(df_upd)
        df_union.persist(StorageLevel.DISK_ONLY)
        df_to_save = df_union

    spark_postgres_service.save_dataframe_to_table(df_to_save, c.TABLE_NAME)
