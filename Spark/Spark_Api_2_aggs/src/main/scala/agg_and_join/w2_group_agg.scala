package agg_and_join

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{count, expr}

object w2_group_agg extends App{
      Logger.getLogger("org").setLevel(Level.ERROR)
      val sparkconf = new SparkConf()
      sparkconf.set("spark.app.name","group aggregates")
      sparkconf.set("spark.master","local[*]")

      val spark = SparkSession.builder()
        .config(sparkconf).getOrCreate()

      val file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/order_data.csv"
      val df = spark.read.format("csv")
          .option("header", true)
          .option("inferSchema", true)
          .option("path", file_path).load()


      /* Using object Expression
      df.groupBy("Country","InvoiceNo").agg(
            count("InvoiceNo").as("invoice_count"),
            functions.sum("Quantity"),
            functions.sum("UnitPrice"),
            functions.sum(expr("Quantity * UnitPrice"))).show()
       */

      /* Using string expression
      df.groupBy("Country","InvoiceNo")
        .agg(expr("sum(Quantity) as Total_quantity"),
              expr("sum(Quantity * UnitPrice)")
        ).show()
       */

      df.createOrReplaceTempView("sales")
      spark.sql("""SELECT country, invoiceno, sum(Quantity) as total_quantity,
        sum(Quantity * UnitPrice) as total_cost
         FROM sales
            GROUP BY country,invoiceNo"""
      ).show()

      spark.stop()


}
