import sys
from random import random
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
    # Test the spark
    spark = SparkSession.builder.appName("Test_spark").master("local[*]").getOrCreate()
    df = spark.createDataFrame([{"hello": "world"} for x in range(1000)])
    df.show(3, False)
