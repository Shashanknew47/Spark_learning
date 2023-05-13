from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,to_date,regexp_extract,sum,expr


spark_conf = SparkConf()
spark_conf.set("spark.app.name","g_aggregates")
spark_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()


file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/order_data.csv"

order_schema = "InvoiceNo Integer,StockCode String ,Description String ,Quantity Integer,InvoiceDate String,UnitPrice Float,CustomerID Integer ,Country String "

df = spark.read.format('csv').schema(order_schema).option('header',True).option('path',file_path).load()
ndf = df.withColumn("date",regexp_extract(col('InvoiceDate'),'(\S+) (\S+)',1))\
        .withColumn("date",to_date(col('date'),'MM-dd-yyyy'))\
        .withColumn("cost",col('Quantity')* col('UnitPrice'))\
        .drop('InvoiceDate').drop('StoackCode')\
        .cache()


"=== Solving through 'column object expression' ==="

country_sales = ndf.groupBy('Country','InvoiceNo').agg(sum('quantity').alias("total_quantity"),
                                           sum(expr("quantity * UnitPrice")).alias("total_value"))

country_sales.show()

"=== Solving through 'column string expression' ==="

country_sales_string = ndf.groupBy("Country","InvoiceNo").agg(expr("sum(Quantity) as total_quantity"),
                                                              expr("sum(Quantity * UnitPrice) as total ") )

country_sales_string.show()

"=== Solving through 'spark sql' ==="

order_table = ndf.createOrReplaceTempView("orders")

result = spark.sql("""SELECT Country as c ,InvoiceNo,sum(Quantity * UnitPrice) as total_c
                        FROM orders
                         GROUP BY Country, InvoiceNo """)
result.show()



ndf.show(6)
