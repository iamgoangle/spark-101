from pyspark.sql import SparkSession

sp = (SparkSession
      .builder
      .appName("GolfTest")
      .getOrCreate())

sc = sp.sparkContext

data = {
    "brand": "Ford",
    "model": "Mustang",
    "year": 1964
}

rdd = sc.parallelize(data)


def p(x):
    print(x)


print("\n\n")
print(rdd.foreach(p))
