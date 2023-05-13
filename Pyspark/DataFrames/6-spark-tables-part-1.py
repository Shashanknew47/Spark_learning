"""
    Advantage of storing data in Spark tables:
    => We will get flexibility to connect with diff. BI tools.

    Spark table have 2 parts:
    => 1) Data : data is stored in spark.sql.warehouse.dir
    => 2) MetaData : spark table metaData is stored in catalog metastore. which is stored in memory (RAM).
                     So, disadvantage is we can loose it. as memory is not a persistant store.
                     This is why we prefer it here Hive Meta Store to add this add .enableHiveSupport property in Sparksession .

"""

from pyspark import SparkConf
from pyspark.sql import SparkSession

my_conf = SparkConf()
my_conf.set("spark.app.name","schema_validation")
my_conf.set("spark.master","local[*]")


spark = SparkSession.\
        builder\
        .config(conf=my_conf)\
        .enableHiveSupport\
        .getOrCreate()

filepath = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/orders.csv"

orderSchema = "order_id Integer, order_date Timestamp, order_customer_id Integer, order_status String"


orderDf = spark.read.format("csv")\
            .option("header",True)\
            .schema(orderSchema)\
            .option('path',filepath).load()


orderDf.write\
.format("csv")\
.mode("overwrite")\
.saveAsTable("order_table")