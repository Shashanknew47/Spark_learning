from pyspark import SparkConf
from pyspark.sql import SparkSession


conf = SparkConf()
conf.set("spark.app.name","trial")
conf.set("spark.master","local[*]")


spark = SparkSession.builder.config(conf=conf).getOrCreate()


filepath = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/orders.csv"

orderSchema = "order_id String, order_date Date, order_customer_id Float, order_status String"

df = spark.read.format("csv")\
    .schema(orderSchema)\
    .option("header",True)\
    .option("path",filepath)\
    .load()



n = df.head(10)
print(n)

