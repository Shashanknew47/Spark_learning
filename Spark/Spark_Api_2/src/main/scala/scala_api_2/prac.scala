package scala_api_2

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object prac extends App {

      val spark_conf = new SparkConf()
      spark_conf.set("spark.app.name","prac")
      spark_conf.set("spark.master","local[*]")

      val spark = SparkSession.builder.config(spark_conf).enableHiveSupport().getOrCreate()

      val f_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/orders.csv"

      val orderSchema = "order_id Int, order_date Timestamp, order_customer_id Int, order_status String"
      val order_df = spark.read.format("csv").schema(orderSchema).option("header",true).option("path",f_path).load()


      order_df.selectExpr("order_id as id","concat(order_status,'_status') as status","order_customer_id * 100 as h").show()




}
