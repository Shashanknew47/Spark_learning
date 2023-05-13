"""
When we read file where data is not seperated with a single delimiter. then we could call
it a unstructured file.

ex.
 cat orders_new.txt
1 2013-07-25 00:00:00.0  11599,CLOSED
2 2013-07-25 00:00:00.0  256,PENDING_PAYMENT
3 2013-07-25 00:00:00.0  12111,COMPLETE

you wil see this file have 3 diff delimiters like. space, '\t', and ','
"""

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract


file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/orders_new.txt"

my_conf = SparkConf()
my_conf.set("spark.app.name","dealing with unsturctured data")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

base_rdd = spark.read.format("text").option("path",file_path).load()

# * when you will see that base rdd. you will notice that data is in single column and column name is value.
# *base_rdd.show()

regex_string = '^(\S+) (\S+) \S+  (\d+),(\w+)'

file_rdd = base_rdd.select(regexp_extract('value',regex_string,1).alias('order_id'),
                           regexp_extract('value',regex_string,2).alias('date'),
                           regexp_extract('value',regex_string,3).alias('customer_order_id'),
                           regexp_extract('value',regex_string,4).alias('status')
                           ).cache()


file_rdd.show()
file_rdd.select("order_id","status").where("order_id < 4").show()
file_rdd.groupBy('status').count().show()
