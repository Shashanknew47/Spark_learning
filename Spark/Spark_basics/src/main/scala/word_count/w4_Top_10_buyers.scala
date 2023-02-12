package word_count

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object w4_Top_10_buyers extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

        val sc = new SparkContext(master = "local[*]", appName = "top_customers")
        val file_name = "/Users/shashankjain/Desktop/Practice/Spark_learning/Spark/Data_sets/customerorders.csv"

        val file = sc.textFile(file_name).map(x => x.split(",")).map(x => (x(0),x(2).toFloat))
//        val customer_total_parchase = file.sortBy(_._1)
         val top_c = file.reduceByKey(_ + _).sortBy(_._2,ascending = false)

         top_c.collect().foreach(println)

}
