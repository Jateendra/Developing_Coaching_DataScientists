import os
import sys
spark_path = r"C:/Users/jatpradh/Documents/Coaching/05_Pyspark/spark-3.0.1-bin-hadoop2.7" # spark installed folder
os.environ['SPARK_HOME'] = spark_path
sys.path.insert(0, spark_path + "/bin")
sys.path.insert(0, spark_path + "/python/pyspark/")
sys.path.insert(0, spark_path + "/python/lib/pyspark.zip")
sys.path.insert(0, spark_path + "/python/lib/py4j-0.10.9-src.zip")

from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import CountVectorizer

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("countVectorizer")\
        .getOrCreate()

documentDF = spark.createDataFrame([
        ("Let's see an example of countVectorizer".split(" "),),
        ("We will use pyspark library my name is my name is  ".split(" "),),
        ("countVectorizer is important for NLP".split(" "),)
    ], ["sentence"])

documentDF.show()

documentDF.show(truncate=False)

df = spark.createDataFrame([(0, ["a", "b", "c"]), (1, ["a", "b", "b", "c", "a"])],["label", "raw"])
cv = CountVectorizer(inputCol="raw", outputCol="vectors")
model = cv.fit(df)
model.transform(df).show(truncate=False)

count_vector = CountVectorizer(inputCol="sentence", outputCol="count_vector")
count_vector

model = count_vector.fit(documentDF)

result = model.transform(documentDF)


result.show(truncate=False)

spark.stop()

