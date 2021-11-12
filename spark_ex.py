import sys
from random import random
from operator import add

from pyspark.sql import sqlContext


if __name__ == "__main__":
    # Test the spark
    df = spark.createDataFrame([{"hello": "world"} for x in range(1000)])
    df.show(3, False)
