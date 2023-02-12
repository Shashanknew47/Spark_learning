package word_count

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object w3_Top_10_worlds extends App{
    Logger.getLogger("org").setLevel(Level.ERROR)


    val sc = new SparkContext(master = "local[*]", appName = "word_count")
    val filePath = "/Users/shashankjain/Desktop/Practice/Linux/prac_fold/text_fold/FilesExercise/poem.txt"

    val input = sc.textFile(filePath)

    val word_count = input.flatMap(_.split(" ")).map((_,1)).reduceByKey((_ + _))

    /*
    val finalCount = word_count.map(x => (x._2,x._1)).sortByKey(false)
    val morethan10 = finalCount.filter(x => x._1 > 10)
     */

    // another better method of sorting is using sortby
    val finalCount = word_count.sortBy(x => x._2)

    finalCount.collect().foreach(println)





}
