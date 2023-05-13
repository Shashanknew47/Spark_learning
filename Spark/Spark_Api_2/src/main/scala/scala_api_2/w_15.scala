package scala_api_2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object w_15 extends App{
        Logger.getLogger("org").setLevel(Level.ERROR)

        case class Logging(level:String, datetime:String)

        def mapper(line:String):Logging = {
                val fields = line.split(",")
                val logging:Logging = Logging(fields(0),fields(1))
                return logging
                }

        val my_conf = new SparkConf()
        my_conf.set("spark.app.name", "my_windows")
        my_conf.set("spark.master", "local[*]")

        val spark = SparkSession.builder.config(my_conf).enableHiveSupport().getOrCreate()


        import spark.implicits._
        val myList =  List("DEBUG,2015-2-6 16:24:07",
                       "WARN,2016-7-26 18:54:43",
                       "INFO,2012-10-18 14:35:19",
                       "DEBUG,2012-4-26 14:26:50",
                       "DEBUG,2013-9-28 20:27:13",
                       "INFO,2017-8-20 13:17:27",
                       "INFO,2015-4-13 09:28:17",
                       "DEBUG,2015-7-17 00:49:27",
                       "DEBUG,2014-7-26 02:33:09"
        )


        val rdd = spark.sparkContext.parallelize(myList)
        val rdd2 = rdd.map(mapper)
        val df1 = rdd2.toDF()
        df1.show()
}
