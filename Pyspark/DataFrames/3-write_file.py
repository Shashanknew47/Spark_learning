from pyspark import SparkConf
from pyspark.sql import SparkSession

my_conf = SparkConf()
my_conf.set("spark.app.name","write-file")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/orders.csv"

orderSchema = "order_id Integer, order_date Timestamp, order_customer_id Integer, order_status String"

orderDf = spark.read.format("csv")\
            .option("header",True)\
            .schema(orderSchema)\
            .option('path',file_path).load()

"""

     when you will write this dataFrame, then number of
     files as per it's partition number
     So, if you will check on it's destination write. You can see that, it is saved in 4 partitions.

     advantage of repartition is, it will bring more parallelism.
"""

dest_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/write_spark/pyspark/orders"
order_part = orderDf.repartition(5)

"""   In case you want to see how many partitions are there  in a dataFrame
      Then you need to first convert it into Rdd. Then with getNumPartition you see how many
      partitions are there in a dataFrame.
      Ex. orderDf_part.rdd.getNumPartitions
"""

# By default file will be saved in Parquet format.
order_part.write.mode("overwrite")\
    .format("csv") \
    .partitionBy("order_status") \
    .option("maxRecordsPerFile",2000) \
    .option("path",dest_path).\
    save()
