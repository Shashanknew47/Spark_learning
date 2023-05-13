package agg_and_join

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object w6_date_and_groupBy extends App{
        val sparkconf = new SparkConf()
        sparkconf.set("spark.app.name","date-groupBy")
        sparkconf.set("spark.master","local[*]")

        val spark = SparkSession.builder().config(sparkconf).getOrCreate()

        val file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/biglog.txt"

        val df = spark.read.format("csv").option("header",true).option("inferSchema",true)
          .option("path",file_path).load()

        // if get confused watch Dataframe session 23 video
        df.createOrReplaceTempView("log_file")
        val result1 = spark.sql(""" SELECT level,
              date_format(datetime,'MMMM') as month,
              cast(date_format(datetime,'M') as int) as month_number,
              count(1)
          FROM
                log_file
          GROUP BY
                GROUPING SETS (
                        (level,month,month_number),
                                ()
                              )
          ORDER BY
                level,month_number """)

        val result2 = result1.drop("month_number")
        // remove null
        val final_result = result2.withColumn("level",expr("coalesce(level,'total')"))

        final_result.show(60)



        spark.stop()
}
