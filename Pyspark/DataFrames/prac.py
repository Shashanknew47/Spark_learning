from pyspark import SparkConf
from pyspark.sql import SparkSession

from pyspark.sql.functions import col,monotonically_increasing_id


my_conf = SparkConf()
my_conf.set("spark.app.name","write-file")
my_conf.set("spark.master","local[*]")


li = [  ('A','bag',10),
        ('A','chain',2),
        ('A','bag',10),
        ('B','news',20),
        ('B','shoe',34),
        ('B','shoe',34),
        ('B','shoe',34)

      ]



spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

df = spark.createDataFrame(li).toDF('part_key','item','quantity')

n_df = df.withColumn("mid",monotonically_increasing_id())

table_name  = n_df.createOrReplaceTempView('main_table')

result = spark.sql("""
with cte as (SELECT
    part_key,mid,
 row_number() over(partition by part_key order by mid) as duplicates

 FROM main_table)

 SELECT * FROM cte;

""").show()