
from pyspark import SparkConf
from pyspark.sql import SparkSession

my_conf = SparkConf()
my_conf.set("spark.app.name","spark_session_conf")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

filepath = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/orders.csv"


"""Note
    In the case of json header is already inbuilt there with schema. So, you don't need to
    put header and inferSchema option here.

    In parquet, orc, avro formats also have inbuilt schema with them. So, don't need to infer it with them.
"""
orderDf = spark.read.format("csv")\
            .option("header",True)\
            .option('inferSchema',True)\
            .option("mode","FAILFAST")\
            .option('path',filepath).load()

rorder_df = orderDf.repartition(4)

big_idDf = rorder_df.where("order_customer_id >10000 and order_id > 100").\
    select("order_id","order_customer_id").\
    persist()

count_id_df = big_idDf.count()
print(count_id_df)

big_idDf.show()
