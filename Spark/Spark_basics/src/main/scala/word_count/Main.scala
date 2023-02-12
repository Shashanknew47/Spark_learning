package word_count

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Main {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "word_count")
    val filePath = "/Users/shashankjain/Desktop/Practice/Linux/prac_fold/text_fold/FilesExercise/poem.txt"

    val input = sc.textFile(filePath)
    val words = input.flatMap(x => x.split(" "))
    val wordMap = words.map(x => (x, 1))
    val finalCount = wordMap.reduceByKey((a, b) => (a + b))

    finalCount.collect.foreach(println)
    scala.io.StdIn.readLine()



  }

}
