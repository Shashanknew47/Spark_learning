package scala_api_2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object w6_Unstructured_data extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val myregex = """(^\S+) (\S+ \S+)  (\S+)\,(\S+)""".r

  case class Orders(order_id:Int,customer_id:Int,order_status:String)

  def parser(line:String) = {
    line match {
      case myregex(order_id, date, customer_id,order_status) => {
        Orders(order_id.toInt, customer_id.toInt, order_status)
      }
    }
  }


  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","handling_unsturctured_data")
  sparkConf.set("spark.master","local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val lines = spark.sparkContext.textFile("/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/orders_new.txt")

  import spark.implicits._

  // As this is big transformation. So, we are caching it
  val orderDs = lines.map(parser).toDS().cache()

  orderDs.printSchema()
  orderDs.groupBy("order_status").count().show()

  spark.stop()




}
