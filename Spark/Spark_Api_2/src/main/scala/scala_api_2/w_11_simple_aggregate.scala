package scala_api_2

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, count, countDistinct, expr}

object w_11_simple_aggregate extends App{
    Logger.getLogger("org").setLevel(Level.ERROR)

    val my_conf = new SparkConf()
    my_conf.set("spark.app.name","aggregate")
    my_conf.set("spark.master","local[*]")

    val spark = SparkSession.builder.config(my_conf).enableHiveSupport().getOrCreate()

    val file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/order_data.csv"

    val base_df = spark.read.format("csv").option("header",true).option("path",file_path)
                  .option("inferSchema",true).load().cache()


    // column object expression

    base_df.select((count("*").as("row_count"))).show()

    // column object expression group by
    base_df.groupBy("country","quantity").agg(functions.sum("unitPrice").alias("uprice"),
        expr("sum(unitPrice * Quantity)").alias("total_cost")
            ).show()


    // object string Expression
    base_df.selectExpr("count(*) as copper","sum(Quantity)","avg(UnitPrice)","count(distinct(InvoiceNo))").show()

    // group by object string Expression

    base_df.groupBy("country","quantity").agg(expr("sum(unitPrice) as u_prince"),
        expr("sum(unitPrice * Quantity)")).show()

    // using spark sql
    base_df.createOrReplaceTempView("orders")

    spark.sql("SELECT sum(Quantity),country  FROM orders GROUP BY country").show()
    spark.stop()


}


