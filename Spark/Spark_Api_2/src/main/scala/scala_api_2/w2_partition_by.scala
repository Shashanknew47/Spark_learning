package scala_api_2

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object w2_partition_by extends App{
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","write_file")
    sparkconf.set("spark.master","local[*]")

    val spark  = SparkSession.builder().config(sparkconf).getOrCreate()
    val file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/orders.csv"

    val orderSchema = "order_id Int, order_date timestamp, order_customer_id Int, order_status String"

    val orderDf = spark.read.format("csv")
      .option("header",true)
      .option("path",file_path).
      schema(orderSchema).
      load()

  val dest_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/write_spark/partion_orders"

  val write_partition_df = orderDf.write.mode(SaveMode.Overwrite)
      .format("csv")
      .partitionBy("order_status")
      .option("maxRecordsPerFile",2000)
      .option("path",dest_path)
      .save()


//    orderDf.show()

}
