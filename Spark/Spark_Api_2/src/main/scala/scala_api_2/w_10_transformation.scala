package scala_api_2

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, unix_timestamp}
import org.apache.spark.sql.types.DateType

/*
Data =   1,"2013-07-25",11599,"CLOSED"
         2,"2014-07-25",256,"PENDING_PAYMENT"
         3,"2013-07-25",11599,"COMPLETE"
         4,"2019-07-25",8827,"CLOSED"
1. I want to create a scala list
2. from the scala list I want to create a dataframe      orderid, orderdate, customerid, status
3. I want to convert orderdate field to epoch timestamp (unixtimestamp) - number of seconds after 1st january 1970
4. create a new column with the name "newid" and make sure it has unique id's
5. drop duplicates - (orderdate , customerid)
6. I want to drop the orderid column
7. sort it based on orderdate -if I want to add a new column or if I want to change the content of a column
    I should be using .withColumn
 */
object w_10_transformation extends App{
      val sparkconf = new SparkConf()
      sparkconf.set("spark.app.name","transformation")
      sparkconf.set("spark.master","local[*]")

      val spark  = SparkSession.builder()
        .config(sparkconf)
        .getOrCreate()

      val raw_data = List(
                          (1,"2013-07-25",11599,"CLOSED"),
                          (2,"2014-07-25",256,"PENDING_PAYMENT"),
                          (3,"2013-07-25",11599,"COMPLETE"),
                          (4,"2019-07-25",8827,"CLOSED")
                                )

      import spark.implicits._
      val df = spark.createDataFrame(raw_data).toDF("order_id","orderdate","customer_id","status")
      // Change the orderdate column into unix_time
      val df2 = df.withColumn("orderdate",unix_timestamp(col("orderdate").cast(DateType)))

    // adding a new column with consecutive values
      val df3 = df2.withColumn("new_id",monotonically_increasing_id())

    // droping duplicates of orderdate & customer_id
    val df4 = df3.dropDuplicates("orderdate","customer_id")

    // droping the orderid column
    val df5 = df4.drop("order_id").sort("orderdate")
      df5.show()
      spark.stop()
}
