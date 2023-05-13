"""
    Task :
    . from the scala list I want to create a dataframe      orderid, orderdate, customerid, status
    . I want to convert orderdate field to epoch timestamp (unixtimestamp) - number of seconds after 1st january 1970
    . create a new column with the name "newid" and make sure it has unique id's
    . drop duplicates - (orderdate , customerid)
    . I want to drop the orderid column
    . sort it based on orderdate
"""
li = [(1,"2013-07-25",11599,"CLOSED"),
      (2,"2014-07-25",256,"PENDING_PAYMENT"),
      (3,"2013-07-25",11599,"COMPLETE"),
      (4,"2019-07-25", 8827,"CLOSED")]

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,unix_timestamp,to_date,monotonically_increasing_id


spark_conf = SparkConf()
spark_conf.set("spark.app.name","prac1")
spark_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

df = spark.createDataFrame(li).toDF('orderid','orderdate','customerid','status')

n_df = df.withColumn("date1",unix_timestamp(to_date(col("orderdate"))))\
                .withColumn("monotonic_increase",monotonically_increasing_id()).\
                drop_duplicates(["orderdate","customerid"])\
                .drop('orderid')\
                .sort('orderdate')



n_df.show()