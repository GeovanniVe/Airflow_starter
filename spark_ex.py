import sys
from random import random
from operator import add

from pyspark.sql import sqlContext


if __name__ == "__main__":
    # Every record of this DataFrame contains the label and
    # features represented by a vector.
    df = sqlContext.createDataFrame(data, ["label", "features"])

    # Set parameters for the algorithm.
    # Here, we limit the number of iterations to 10.
    lr = LogisticRegression(maxIter=10)

    # Fit the model to the data.
    model = lr.fit(df)

    # Given a dataset, predict each point's label, and show the results.
    model.transform(df).show()
