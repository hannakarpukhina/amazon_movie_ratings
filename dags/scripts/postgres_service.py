class PostgresService:
    def __init__(self, spark_session, postgres_host, properties):
        self.spark_session = spark_session
        self.postgres_host = postgres_host
        self.properties = properties

    def check_table_exists(self, table_name):
        table_exist = False
        try:
            self.spark_session.read.jdbc(
                self.postgres_host, table_name, properties=self.properties
            )
            table_exist = True
        except:
            pass
        return table_exist

    def save_dataframe_to_table(self, df, table_name):
        df.write.jdbc(
            self.postgres_host, table_name, mode="overwrite", properties=self.properties
        )

    def get_dataframe_from_table(self, table_name):
        df = self.spark_session.read.jdbc(
            self.postgres_host, table_name, properties=self.properties
        )
        return df
