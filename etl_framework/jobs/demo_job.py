from etl_framework.utils.processor import Processor


class DemoJob(Processor):
    target_table = 'demo_job'

    extract_order = [
        'extract_movies',
        'extract_ratings'
    ]

    transform_order = [
        'transform_aggregated_movies',
        'transform_top_movies'
    ]

    def transform_aggregated_movies(self):
        transform_query = """
        SELECT
            movieId,
            round(avg(rating), 2) AS avg_rating,
            count(rating) AS ratings_count
        FROM ratings
        GROUP BY movieId
        """
        df = self.spark.sql(transform_query)
        df.createOrReplaceTempView('aggregated_movies')

    def transform_top_movies(self):
        transform_query = """
        SELECT 
            m.title, 
            a.avg_rating, 
            a.ratings_count 
        FROM aggregated_movies a 
        LEFT JOIN movies m ON a.movieId = m.movieId
        WHERE a.ratings_count >= 100
        ORDER BY a.avg_rating DESC
        """
        df = self.spark.sql(transform_query)
        df.createOrReplaceTempView(self.target_table)
