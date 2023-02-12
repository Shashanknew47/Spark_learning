package scala_api_2

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object prac extends App{

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "write_file")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  val file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/orders"

  val orderDf = spark.read.option("path",file_path).load
  orderDf.printSchema()
  orderDf.show()






}
