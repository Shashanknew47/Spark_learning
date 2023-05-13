package word_count

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
object summary extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "word_count")
  val filePath = "/Users/shashankjain/Desktop/Practice/Linux/prac_fold/text_fold/FilesExercise/poem.txt"


  val input = sc.textFile(filePath)



}
