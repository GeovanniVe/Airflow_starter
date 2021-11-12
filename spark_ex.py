import sys
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonHelloWorld")\
        .getOrCreate()

    # Test the spark
    df = spark.createDataFrame([{"hello": "world"} for x in range(10)])
    df.show(3, False)

    spark.stop()
