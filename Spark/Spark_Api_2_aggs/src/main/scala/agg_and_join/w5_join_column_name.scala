package agg_and_join
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object w5_join_column_name extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkconf = new SparkConf()
  sparkconf.set("spark.app.name", "window aggregate")
  sparkconf.set("spark.master", "local[*]")

  val spark = SparkSession.builder().config(sparkconf).getOrCreate()

  val orders_file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/orders.csv"
  val customer_file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/customers.csv"

  val orders_df = spark.read.format("csv").option("inferSchema", true)
    .option("header", true)
    .option("path", orders_file_path)
    .load()

    // How to change the column name
    val new_order_df = orders_df.withColumnRenamed("order_id","customer_order_id")

  val customers_df = spark.read.format("csv").option("inferSchema", true)
    .option("header", true)
    .option("path", customer_file_path)
    .load()


  /*
   In case you are doing a join and there are columns in 2 tables have same name
   then you should change the name of those similar columns.
            or
    you should drop one of columns with df.drop('col_name') to prevent select ambiguous col names
   */

  val join_condition = new_order_df.col("order_customer_id") === customers_df.col("customer_id")

  // this drop will have ambiguous column errors in select and save space also.As same data is repeted in 2 columns
  val join_df = new_order_df.join(customers_df,join_condition,"left")
    .drop(orders_df.col("order_customer_id")).sort("customer_id")
    .select("customer_id","order_status","customer_fname")


  //Now we want to change the null value of customer_id into -1
  val no_null_df = join_df.withColumn("customer_id",expr("coalesce(customer_id,-1)"))
  no_null_df.select("customer_id","order_status","customer_fname").show()

  spark.stop()


}
