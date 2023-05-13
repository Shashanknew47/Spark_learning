"""
    There are 2 ways to create udfs in Spark:
    1) Column object expression : In this case function will not be registered in catalog
    2) SQL Expression : In this case function will be registered in catalog. So, in this case we can use it with Spark sql also.
"""

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,udf, StringType


my_conf = SparkConf()
my_conf.set("spark.app.name","columns")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()


file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/dataset1"
name_schema = "name String, age Integer, city String"
student_df = spark.read.format('csv').schema(name_schema).option("path",file_path).load()

name_df = student_df.toDF("name","age","city").cache()

def age_check(age):
    if age > 18:
        return 'T'
    else:
        return 'F'

parseAgeFunction = udf(age_check,StringType())

df = name_df.withColumn("adult",parseAgeFunction('age'))

df.show()
