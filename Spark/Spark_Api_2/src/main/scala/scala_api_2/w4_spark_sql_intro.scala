package scala_api_2

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala_api_2.w3_avro_file.spark

object w4_spark_sql_intro extends App{
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","spark_sql")
    sparkconf.set("spark.master","local[2]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    val file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/write_spark/avro_partion_orders"
    val orderSchema = "order_id Int, order_date timestamp, order_customer_id Int, order_status String"

    
    val orderDf = spark.read.format("avro")
      .option("path", file_path)
      .load()

    orderDf.createOrReplaceTempView("orders")
    val sql_result = spark.sql("SELECT * FROM orders")
    sql_result.show()
    spark.stop()

}
