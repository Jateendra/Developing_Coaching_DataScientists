import os
import sys
spark_path = r"C:/Users/jatpradh/Documents/Coaching/05_Pyspark/spark-3.0.1-bin-hadoop2.7" # spark installed folder
os.environ['SPARK_HOME'] = spark_path
sys.path.insert(0, spark_path + "/bin")
sys.path.insert(0, spark_path + "/python/pyspark/")
sys.path.insert(0, spark_path + "/python/lib/pyspark.zip")
sys.path.insert(0, spark_path + "/python/lib/py4j-0.10.9-src.zip")

import pyspark
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.mllib.stat import Statistics

from pyspark import SparkContext
from pyspark import SparkConf

if __name__ == "__main__":
    sc = SparkSession .builder\
        .appName("checkCorrealtion")\
        .getOrCreate()

df = sc.read.csv("C:/Users/jatpradh/Documents/Coaching/05_Pyspark/diabetes.csv",header=True)

df

df.corr

df.count

df.columns

df.describe().show()

df.show()

df.describe()

df.distinct()

type(df)

# we need to convert dataframe intp a RDD to check for correlation
col_names = df.columns
features = df.rdd.map(lambda row: row[0:])

# checking correaltion using pearson method
corr_mat=Statistics.corr(features, method="pearson")
corr_df = pd.DataFrame(corr_mat)
corr_df.index, corr_df.columns = col_names, col_names

corr_df.columns

corr_df.index

corr_df

# end the spark session
sc.stop()

