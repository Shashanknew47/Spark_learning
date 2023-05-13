package agg_and_join

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window


object w3_window_agg extends App{
      Logger.getLogger("org").setLevel(Level.ERROR)
      val sparkconf = new SparkConf()
      sparkconf.set("spark.app.name","window aggregate")
      sparkconf.set("spark.master","local[*]")

      val spark = SparkSession.builder().config(sparkconf).getOrCreate()

      val file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/windowdata.csv"

      val df =  spark.read.format("csv").option("inferSchema",true).option("path",file_path).load()
      val df1 = df.toDF("country","weeknum","numinvoices","totalquantity","invoicevalue")

      val my_window = Window.partitionBy("country")
        .orderBy("weeknum")
        .rowsBetween(Window.unboundedPreceding,Window.currentRow)

      val my_df = df1.withColumn("RunningTotal",functions
                                  .sum("invoicevalue")
                                  .over(my_window))

      my_df.show()


}
