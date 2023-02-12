package word_count

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

// When we have list of values like [1,2,1,1,3,3,2] then countByValue will give us [(1,3),(2,2),(3,2)]
// Count by value is an action means. if you are using this. Make sure that is the last step.

object w6_countbyvalue extends App{

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","movie_star_count")
    val filePath = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/moviedata.data"

    val movie_star_rdd = sc.textFile(filePath).map(x => x.split("\t")(2))
    movie_star_rdd.countByValue().foreach(println)

}
