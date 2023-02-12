package word_count

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

object w5_avg_for_each_age extends App {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext(master = "local[*]", appName = "avg_age_connections")

    val file_name = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/friendsdata.csv"
    val input = sc.textFile(file_name).map(x => (x.split("::").toList)).map(x => (x(2), x(3).toInt))
    val new_format = input.map(x => (x._1, (x._2, 1))).sortBy(x => x._1)

    def sum_count(x:(Int,Int),y:(Int,Int)):(Int,Int) = {
        ((x._1 + y._1),(x._2 + y._2))
    }
    val final_format = new_format.reduceByKey(sum_count).persist(StorageLevel.DISK_ONLY_2)
    val avg_connections = final_format.map(x => (x._1,x._2._1.toFloat/x._2._2))

//    avg_connections.collect.foreach(println)

    val new_new_format = final_format.map(x => x)
    new_new_format.collect.foreach(println)


//    scala.io.StdIn.readLine()

    }


