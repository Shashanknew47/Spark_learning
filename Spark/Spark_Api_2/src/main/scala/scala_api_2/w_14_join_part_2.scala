package scala_api_2

import org.apache.log4j.{Logger,Level}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, expr}

/*
    In case you have 2 same column name in the 2 tables which you are joining.
    So, either it's better to rename them before making a join.
    or if in case data is also the same then it's better to drop apart from one.
 */

object w_14_join_part_2 extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  val my_conf = new SparkConf()
  my_conf.set("spark.app.name", "my_windows")
  my_conf.set("spark.master", "local[*]")

  val spark = SparkSession.builder.config(my_conf).enableHiveSupport().getOrCreate()

  val customer_file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/customers.csv"
  val order_file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/orders.csv"

  val customer_df = spark.read.format("csv").option("header", true).option("inferSchema", true).option("path", customer_file_path).load()
  val order_df = spark.read.format("csv").option("header", true).option("inferSchema", true).option("path", order_file_path).load()

  /* Now we have changed the column. to, check one error which will occur
  val c_order_df = order_df.withColumnRenamed("order_customer_id","customer_id")
  c_order_df.show()


  val join_condition = customer_df.col("customer_id") === c_order_df.col("customer_id")
  val joinType = "outer"

   Now you will notice that customer_id column is presented in both dataFrames
  val join_df = customer_df.join(c_order_df,join_condition,joinType)

   This below action will be executed in error bez of ambiguious col
  join_df.select("customer_id","customer_fname","customer_state","order_status").show()

   So, a better approach is first rename one of column then join it. Then drop one of the column
   */

  val join_conditon = customer_df.col("customer_id") === order_df.col("order_customer_id")
  val join_type = "outer" // full join


  val join_df = order_df.join(customer_df, join_conditon, join_type).drop("order_customer_id")


  val null_join_df = join_df.withColumn("customer_id",expr("coalesce(customer_id,-1)"))
    .select("order_id","order_date","customer_id","customer_fname")

  null_join_df.show()


//    join_df.select("order_id","order_date")
}
