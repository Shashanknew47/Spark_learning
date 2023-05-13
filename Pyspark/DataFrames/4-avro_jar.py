from pyspark import SparkConf
from pyspark.sql import SparkSession


# To store data in avro you need to add a external dependency, by adding a new jar
# Download the jar from maven repository. : type Spark avro and as per your version download the jar.

jar_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/write_spark/pyspark/spark-avro_2.12-3.3.1.jar"


my_conf = SparkConf()
my_conf.set("spark.app.name","write-file")
my_conf.set("spark.master","local[*]")
my_conf.set("spark.jars",jar_path)     # add the jar here in conf settings 

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/orders.csv"

orderSchema = "order_id Integer, order_date Timestamp, order_customer_id Integer, order_status String"


orderDf = spark.read.format("csv")\
            .option("header",True)\
            .schema(orderSchema)\
            .option('path',file_path).load()



dest_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/write_spark/pyspark/avro/orders"

orderDf.write.mode("overwrite")\
    .format("avro") \
    .option("maxRecordsPerFile",10000) \
    .option("path",dest_path).\
    save()
