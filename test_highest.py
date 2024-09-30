import datetime
import unittest
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual

from main import highest_close_price_year


class HighestClosingPriceTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_highest_close(self):
        sample_data = [{'date': datetime.date(year=2024, month=1, day=23), 'open': 1.0, 'high' : 2.0, 'low': 1.5, 'close': 3.4},
                       {'date': datetime.date(year=2024, month=1, day=24), 'open': 1.0, 'high' : 2.0, 'low': 1.5, 'close': 3.2},
                       {'date': datetime.date(year=2025, month=1, day=24), 'open': 1.0, 'high' : 2.0, 'low': 1.5, 'close': 3.2}]

        original_df = self.spark.createDataFrame(sample_data)
        expected_data = [{'date': datetime.date(year=2024, month=1, day=23), 'open': 1.0, 'high' : 2.0, 'low': 1.5, 'close': 3.4},
                         {'date': datetime.date(year=2025, month=1, day=24), 'open': 1.0, 'high' : 2.0, 'low': 1.5, 'close': 3.2}]

        expected_df = self.spark.createDataFrame(expected_data)

        transformed_df = highest_close_price_year(original_df, self.spark)

        assertDataFrameEqual(transformed_df, expected_df)

