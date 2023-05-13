from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,TimestampType,StringType
import datetime

my_conf = SparkConf()
my_conf.set("spark.app.name","schema_validation")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

filepath = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/orders.csv"
"""
There are 2 ways to apply schema :
    => Struct Type
    => DDL String
"""

"""
For this kind of Struct Type you need to import
from pyspark.sql.types import StructType,StructField,IntegerType,TimestampType,StringType
from 2-SchemaDf import orderSchema
orderSchema = StructType([
    StructField("order_id",IntegerType()),
    StructField("order_date",TimestampType()),
    StructField("order_customer_id",IntegerType()),
    StructField("order_status",StringType())
])
"""
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
