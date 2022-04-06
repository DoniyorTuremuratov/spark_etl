from pyspark.sql import SparkSession

from etl_framework.dependencies.logger_utils import get_logger
from etl_framework.utils.extractor import Extractor

logger = get_logger()


class Processor:
    extract_order = []
    transform_order = []
    load_order = ['load_final_table']

    def __init__(self, spark: SparkSession, target_table: str):
        self.spark = spark
        self.target_table = target_table
        self.extractor = Extractor(self.spark)

    def run_method_group(self, method_group: str):
        method_class = self.get_method_class(method_group)
        attrs = [attr for attr in method_class.__dir__()]
        for name in self.get_method_group(method_group):
            if name not in attrs:
                raise ValueError(f"Method {name} is not defined.")
            method = method_class.__getattribute__(name)
            if not callable(method):
                raise ValueError(f'Method {name} is not callable.')
            logger.info(f"Running {name}.")
            method()

    def get_method_class(self, method_group: str):
        method_classes = {
            'extract': self.extractor,
            'transform': self,
            'load': self
        }
        return method_classes[method_group]

    def get_method_group(self, method_group: str):
        method_groups = {
            'extract': self.extract_order,
            'transform': self.transform_order,
            'load': self.load_order
        }
        return method_groups[method_group]

    def load_final_table(self):
        logger.info('Loading data to the disk...')
        self.spark.table(self.target_table).show(truncate=False)
        logger.info("Process is complete.")

    def extract(self):
        self.run_method_group('extract')

    def transform(self):
        self.run_method_group('transform')

    def load(self):
        self.run_method_group('load')

    def run(self):
        self.extract()
        self.transform()
        self.load()
