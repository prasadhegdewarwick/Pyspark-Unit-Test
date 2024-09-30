from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
from pyspark.sql.types import DateType
from pyspark.sql.types import DoubleType
import os
from pyspark.sql.functions import *
from pyspark.sql import Window


def create_spark_session():
    return SparkSession \
        .builder \
        .appName("Pyspark-test") \
        .master("local[*]") \
        .getOrCreate()


def get_file_path(file):
    return os.path.abspath(os.path.join('./data/', file + '.csv'))


def load_csv_file(file_name, spark):
    full_path = get_file_path(file_name)
    df = spark.read.option("header", True).csv(full_path)

    return df.select(
        df['Date'].cast(DateType()).alias("date"),
        df['Open'].cast(DoubleType()).alias('open'),
        df['High'].cast(DoubleType()).alias('high'),
        df['Low'].cast(DoubleType()).alias('low'),
        df['Close'].cast(DoubleType()).alias('close')
    )


def highest_close_price_year(df, spark):
    window = Window.partitionBy(year(df['date'])).orderBy(df['close'].desc())
    df_higest = df.withColumn("rnk", row_number().over(window)) \
        .filter(col('rnk') == 1). \
        drop(col('rnk'))

    return df_higest


if __name__ == "__main__":
    spark = create_spark_session()
    df_appl = load_csv_file('AAPL', spark)

    df_highest = highest_close_price_year(df_appl,spark)
    df_highest.show()

