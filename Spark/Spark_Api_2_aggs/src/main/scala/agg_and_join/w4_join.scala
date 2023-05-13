package agg_and_join


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object w4_join extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkconf = new SparkConf()
  sparkconf.set("spark.app.name", "window aggregate")
  sparkconf.set("spark.master", "local[*]")

  val spark = SparkSession.builder().config(sparkconf).getOrCreate()

  val orders_file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/orders.csv"
  val customer_file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/customers.csv"

      val orders_df = spark.read.format("csv").option("inferSchema",true)
        .option("header",true)
        .option("path",orders_file_path)
        .load()

//      orders_df.show()

      val customers_df = spark.read.format("csv").option("inferSchema",true)
        .option("header",true)
        .option("path",customer_file_path)
        .load()

//      customers_df.show()

      val join_condition = orders_df.col("order_customer_id") === customers_df.col("customer_id")
      // here join_type could be : inner,left,right,outer
      val join_type = "right"

      val joinDf = orders_df.join(customers_df,join_condition,join_type).sort("customer_id")
      joinDf.show()
      /* Customer who did not place any orders
      joinDf.createOrReplaceTempView("customer_table")
      spark.sql("SELECT * FROM customer_table WHERE order_customer_id is null").show()
       */

      spark.stop()

}
