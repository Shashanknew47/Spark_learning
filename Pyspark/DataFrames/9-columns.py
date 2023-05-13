from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

my_conf = SparkConf()
my_conf.set("spark.app.name","columns")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

"""
  we have file dataset1, which does not have any column name. So, we can use toDf method to provide column name

sumit,30,bangalore
kapil,32,hyderabad
sathish,16,chennai
"""



file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/dataset1"
name_schema = "name String, age Integer, city String"
student_df = spark.read.format('csv').schema(name_schema).option("path",file_path).load()

name_df = student_df.toDF("name","age","city").cache()


name_df.show()
name_df.printSchema()

#  adding a new column with twice age
df = name_df.withColumn("double_age",col("age").cast("Integer")*2)
df.show()

# changing the value of existing column
new_df = df.withColumn("age",col("age").cast("Integer")*100)
new_df.show()
