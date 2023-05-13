package agg_and_join

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, count, countDistinct}


// calculate total_number_of_rows, totalQuantity, avgUnitPrice, numberOfUniqueInvoices
// with object expression

object w1_agg extends App{
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","aggregation")
    sparkconf.set("spark.master","local[*]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    val file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/order_data.csv"
    val df = spark.read.format("csv")
      .option("header",true)
      .option("inferSchema",true)
      .option("path",file_path).load()


    //"solving with ==== column object Expression ===="
    /*
    df.select(
        count("*").as("RowCount"),
        functions.sum("Quantity").as("TotalQuantity"),
        avg("UnitPrice").as("AvgPrice"),
        countDistinct("InvoiceNo").as("countDistinct")
    ).show()
     */

    /* Solving same with string expressions
    df.selectExpr(
        "count(*) as RowCount",
        "sum(Quantity) as TotalQuantity",
        "avg(UnitPrice) as AvgPrice",
        "count(Distinct(InvoiceNo)) as Countdistinct"
    ).show()
     */


    // solving with sparkSQL
    df.createOrReplaceTempView("sales")
    spark.sql("SELECT count(*) as Rowcount," +
      "sum(Quantity)," +
      "avg(UnitPrice)," +
      "count(distinct(InvoiceNo)) as dintinctInvoiceCount" +
      " FROM sales").show()


    spark.stop()

}
