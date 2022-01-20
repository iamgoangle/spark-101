import pyspark
import time

from pyspark.sql import SparkSession

sp = (
    SparkSession
    .builder
    .appName("GolfDemo")
    .getOrCreate()
)

df = sp.read.option("header", "true").csv("cereal.csv")

# show data as a table
# df.show()

# print the schema
# df.printSchema()

df.groupBy("name", "calories").count().show()

time.sleep(30)