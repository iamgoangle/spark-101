from doctest import master
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

# create spark session object
spark = (SparkSession
         .builder
         .appName("GolfTest")
         .getOrCreate())

# create a spark context
sp = spark.sparkContext

data = ["test A", "test B"]
rdd = sp.parallelize(data)

# print("== rdd ==", rdd)
# print(rdd.collect())
# print(rdd.count())
# print(rdd.first())

# create a dataframe
data2 = [("James", "", "Smith", "36636", "M", 3000),
         ("Michael", "Rose", "", "40288", "M", 4000),
         ("Robert", "", "Williams", "42114", "M", 4000),
         ("Maria", "Anne", "Jones", "39192", "F", 4000),
         ("Jen", "Mary", "Brown", "", "F", -1)
         ]

schema = StructType([
    StructField("firstname", StringType(), True),
    StructField("middlename", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", IntegerType(), True)
])

df = spark.createDataFrame(data=data2, schema=schema)
df.printSchema()
df.show(truncate=False)
