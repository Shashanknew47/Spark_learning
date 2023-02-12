package scala_api_2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

case class Person(name:String,age:Int,city:String)

object w8_udf_Add_column extends App{

      Logger.getLogger("org").setLevel(Level.ERROR)

      def age_check(age:Int) = {
            if (age > 18) "Y" else "N"
      }

      val sparkconf = new SparkConf()
      sparkconf.set("spark.app.name","udf_columns")
      sparkconf.set("spark.master","local[*]")

      val spark = SparkSession.builder().config(sparkconf).getOrCreate()

      val file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/dataset1"


      val df  = spark.read.format("csv")
        .option("inferSchema",true)
        .option("path",file_path)
        .load()

      // ad dataset1 don't have column name. So give them column name with df.toDF()
      val df_wcolumns:Dataset[Row] = df.toDF("name","age","city")

      /* In case you want to create Dataset from df
      import spark.implicits._
      val df_dset = df_wcolumns.as[Person]

      converting back from dataset to dataFrame
      val df_origin = df_dset.toDF()
       */

      val parseAgeFunction = udf(age_check(_:Int):String)

      /* In this register the function with driver, driver will serialize the
      function with the driver and driver will send to each executor.
       */
      val df2 = df_wcolumns.withColumn("adult",parseAgeFunction(col("age")))

      df2.show()
      spark.stop()

}
