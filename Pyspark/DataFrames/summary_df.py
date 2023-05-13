
from pyspark import SparkConf
from pyspark.sql import SparkSession


# Entry of a Spark application by creating sparkSession
config = SparkConf()
config.set('spark.app.name','summary')
config.set('spark.master','local[*]')

"""
MetaData : spark table metaData is stored in catalog metastore. which is stored in memory (RAM).
                     So, disadvantage is we can loose it. as memory is not a persistant store.
                     This is why we prefer it here Hive Meta Store to add this add .enableHiveSupport property in Sparksession

"""


spark = SparkSession.builder\
        .config(conf=config)\
        .enableHiveSupport\
        .getOrCreate()

filepath = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/orders.csv"


# TODO - go to this link https://spark.apache.org/docs/latest/sql-ref-datatypes.html
# ! Go to type table
# ! SELECT Python
# ! In the column 'Api to access or create a datatype' remove last part Type() ex. if there is IntegerType() then take
# ! only Integer and place in the below schema.

orderSchema = "order_id String, order_date Timestamp, order_customer_id float, order_status string"


orderDf = spark.read.format("csv")\
            .option("header",True)\
            .schema(orderSchema)\
            .option('path',filepath).load()


print(orderDf.schema)


orderDf.show()

"""   In case you want to see how many partitions are there  in a dataFrame
      Then you need to first convert it into Rdd. Then with getNumPartition you see how many
      partitions are there in a dataFrame.
      Ex. orderDf_part.rdd.getNumPartitions
"""
orderDf_part_4 =  orderDf.repartition(4)

dest_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/write_spark/pyspark/orders"

orderDf_part_4.write.mode("overwrite")\
    .format("csv")\
    .partitionBy("order_status")\
    .option("maxRecordsPerFile",2000)\
    .option("path",dest_path).\
    save()
