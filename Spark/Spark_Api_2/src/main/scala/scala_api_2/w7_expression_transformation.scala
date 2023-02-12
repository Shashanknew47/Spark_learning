package scala_api_2

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object w7_expression_transformation extends App{
        val sparkconf = new SparkConf()
        sparkconf.set("spark.app.name","transformation")
        sparkconf.set("spark.master","local[*]")

        val spark = SparkSession.builder().config(sparkconf).getOrCreate()

  val file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/write_spark/avro_partion_orders"
  val orderDf = spark.read.format("avro")
          .option("path",file_path)
          .load()

        /* There are 2 ways we can select columns of a dataFrame
            * By string ex.  df.select("col1","col2")
            * By column object (column, col, - scala specific $,'col ex. :  df.select(column("order_id",'order_status)
            Note: diff column object notation could be mixed but string and object notation can't be mixed
         */

//        orderDf.select("order_id","order_customer_id","order_status").show()
          orderDf.show()
          import spark.implicits._
          // to use column & col you need new imports
          // to use $ and ' you need to import spark_implicits._

          val newDf = orderDf.select(column("order_id"),col("order_status"),$"order_date",'order_customer_id)

          /* adding a transformations in in order_status
              * transformation: if we have a value Processing then we will add 'status' in that:  Processing_status
              * we can use expr (expression) with column objects
           */


//          newDf.select(col("order_id"),expr("concat(order_status,'_status')")).show()


          // with selectExpr you can mix column string with expressions. this is the best method to transform
          val transformed_df = newDf.selectExpr("order_id","concat(order_status,'_status')")
          transformed_df.show(false)


          spark.stop()

}
