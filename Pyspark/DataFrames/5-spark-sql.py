from pyspark import SparkConf
from pyspark.sql import SparkSession

my_conf = SparkConf()
my_conf.set("spark.app.name","spark-sql")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

orderSchema = "order_id Integer, order_date Timestamp, order_customer_id Integer, order_status String"

filepath = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/orders.csv"

order_df = spark.read.format("csv").option("header",True).schema(orderSchema).option('path',filepath).load()

order_spark_sql = order_df.createOrReplaceTempView("orders")

sql_result = spark.sql("SELECT * FROM orders").show()
