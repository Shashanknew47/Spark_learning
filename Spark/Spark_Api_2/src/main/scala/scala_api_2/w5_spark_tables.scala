package scala_api_2

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object w5_spark_tables extends App{
   val sparkconf = new SparkConf()
   sparkconf.set("spark.app.name","spark_tables")
   sparkconf.set("spark.master","local[*]")

  // Note : first donwlond spark project hive library from maven for Hive support
    val spark = SparkSession.builder()
      .config(sparkconf)
      .enableHiveSupport()      // to store meta data in Hive
      .getOrCreate()

  val file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/write_spark/avro_partion_orders"
  val orderSchema = "order_id Int, order_date timestamp, order_customer_id Int, order_status String"


  val orderDf = spark.read.format("avro")
    .option("path", file_path)
    .load()

  // if you want to store spark table in a customize database
  spark.sql("CREATE DATABASE IF NOT EXISTS retail")

  // CReating the table form df
  orderDf.write.format("csv")
    .mode(SaveMode.Overwrite)
    .bucketBy(4,"order_customer_id")
    .sortBy("order_customer_id")
    .saveAsTable("retail.orders")

  spark.catalog.listTables("retail").show()

  spark.stop()

}
