package scala_api_2
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, expr}

/* There are 2 types of join
  - simple join (Shuffle sort merge join)
  - Broadcast join
 */


object w_13_join_part_1 extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  val my_conf = new SparkConf()
  my_conf.set("spark.app.name", "my_windows")
  my_conf.set("spark.master", "local[*]")

  val spark = SparkSession.builder.config(my_conf).enableHiveSupport().getOrCreate()

    val customer_file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/customers.csv"
    val order_file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/orders.csv"

    val customer_df = spark.read.format("csv").option("header",true).option("inferSchema",true).option("path",customer_file_path).load()
    val order_df = spark.read.format("csv").option("header",true).option("inferSchema",true).option("path",order_file_path).load()

    /* to make join you need you need 2 things:
        1) join condition
        2) join type
     */


   val join_condiiton =  customer_df.col("customer_id") === order_df.col("order_customer_id")
   val join_type = "outer"   // full join

    val join_df =  order_df.join(customer_df,join_condiiton,join_type)
    join_df.select("order_id","order_status","customer_fname","order_date").where(col("order_status").isin("COMPLETE",
     "PROCESSING","PENDING_PAYMENT"))

      .show(100)

      order_df.createOrReplaceTempView("orders")
      customer_df.createOrReplaceTempView("customers")

      spark.sql(
        """SELECT order_id,customer_fname,order_status
          FROM orders full outer join customers on orders.order_customer_id = customers.customer_id
          WHERE orders.order_status = 'COMPLETE'
          """).show()

  spark.stop()
}
