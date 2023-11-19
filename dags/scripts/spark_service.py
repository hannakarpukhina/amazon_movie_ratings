class SparkService:
    def __init__(self, spark_session):
        self.spark_session = spark_session

    def get_dataframe_from_file(self, path, filetformat, schema):
        df_gcs = self.spark_session.read.format(filetformat).schema(schema).load(path)
        return df_gcs
