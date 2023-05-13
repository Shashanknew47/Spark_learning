"""
    => spark table metaData is stored in catalog metastore. which is stored in memory (RAM).
        So, disadvantage is we can loose it. as memory is not a persistant store.
        This is why we prefer it here Hive Meta Store.

        you just need to add in .enableHiveSupport() property in SparkSession to store metadata in Hive

"""

#! with spark tables you take advantage of bucket functionality also.

from pyspark import SparkConf
from pyspark.sql import SparkSession

my_conf = SparkConf()
my_conf.set("spark.app.name","schema_validation")
my_conf.set("spark.master","local[*]")


#! add .enableHiveSupport property to store meta data of spark table in Hive Meta data.
#! you will see a new folder metastore_db will be created to store the metastore
spark = SparkSession.builder\
        .config(conf=my_conf)\
        .enableHiveSupport()\
        .getOrCreate()

filepath = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/orders.csv"

orderSchema = "order_id Integer, order_date Timestamp, order_customer_id Integer, order_status String"


orderDf = spark.read.format("csv")\
            .option("header",True)\
            .schema(orderSchema)\
            .option('path',filepath).load()


# ! By default it will be saved in spark-warehouse directory.
# ! in case you want to create a seperate customized warehouse then use below command
"=================================="
# ! spark.sql("create database if not exist retail") and in saveAstable("retail.order2") command provide this database name
# ! with customised table name  as argument
"==================================="
# ! So, a new database with the name of retail will be created inside spark-warehouse directory.

spark.sql("CREATE DATABASE IF NOT EXISTS retail")

# it is always better to store the data using bucks and sorting. So, use bucketBy and sortBy
# in bucketBy takes 2 arguments first is number of buckets and second is name of column of which you want to apply bucket
# bucketing can only be used when we are dealing with spark tables.

orderDf.write\
.format("csv")\
.mode("overwrite")\
.bucketBy(4,"order_customer_id")\
.saveAsTable("retail.order4")

