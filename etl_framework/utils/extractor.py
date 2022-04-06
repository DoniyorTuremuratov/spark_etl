from pyspark.sql import SparkSession


class Extractor:

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def extract_movies(self):
        df = self.spark.read.parquet('dataset/movies.parquet')
        df.createOrReplaceTempView('movies')

    def extract_ratings(self):
        df = self.spark.read.parquet('dataset/ratings.parquet')
        df.createOrReplaceTempView('ratings')
