import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import count


my_conf = SparkConf()
my_conf.set("spark.app.name","prac")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).enableHiveSupport().getOrCreate()

file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/orders.csv"

df = spark.read.format("csv").option("inferSchema",True).option("path",file_path).option("header",True).load()


df.groupBy("order_status").sum("order_id").withColumnRenamed("sum(order_id)","order_count").sort("order_count").show()