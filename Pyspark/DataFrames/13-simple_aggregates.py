from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,regexp_extract,to_date,count,sum,avg,countDistinct


spark_conf = SparkConf()
spark_conf.set("spark.app.name","aggregates")
spark_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()


file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/order_data.csv"

order_schema = "InvoiceNo Integer,StockCode String ,Description String ,Quantity Integer,InvoiceDate String,UnitPrice Float,CustomerID Integer ,Country String "


df = spark.read.format('csv').schema(order_schema).option('header',True).option('path',file_path).load()

ndf = df.withColumn("date",regexp_extract(col('InvoiceDate'),'(\S+) (\S+)',1))\
        .withColumn("date",to_date(col('date'),'MM-dd-yyyy'))\
        .withColumn("cost",col('Quantity')* col('UnitPrice'))\
        .drop('InvoiceDate').cache()


"Doing simple aggregation by 3 styles"

"""Column Object Expression"""
# For this we need to import all count,sum, avg, counDistict explicitly form pyspark.sql.functions
ndf.select(
    count("*").alias("Rowcount"),
    sum("Quantity").alias("total_quantity"),
    avg("UnitPrice").alias("avg_price"),
    countDistinct("InvoiceNo").alias("invoice_count")
).show()


"Column String Expression"

ndf.selectExpr(
        "count(*) as Rowcount",
        "sum(Quantity) as total_quantity",
        "avg(UnitPrice) as avg_price",
        "count(Distinct(InvoiceNo)) as invoice_count").show()


"Spark SQL"
final_df = ndf.createOrReplaceTempView("orders")

result = spark.sql("""
                SELECT  count(*) as row_count, sum(Quantity) as total_quantity,
                        avg(UnitPrice) as avg_price,count(distinct(InvoiceNo)) as invoice_count
                FROM orders
         """).show()


ndf.show(5)