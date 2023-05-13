package word_count

import org.apache.spark.SparkContext
object w1_reading_file extends App{

    /* Here * inside local represents that use all the cores. Here you can mention a number
     which will specify number of cores to be used in Spark. */
    val sc = new SparkContext("local[*]", "word_count")

    val filePath = "/Users/shashankjain/Desktop/Practice/Linux/prac_fold/text_fold/FilesExercise/poem.txt"

    // textFile method will load the file in the RDD.
    val input = sc.textFile(filePath)



    /* here after using collect action. it will create an array of lines of file.
       this array of lines will not be visible by intellij. So, to see this use command line
       spark-shell.
    */

    val Rdd_array_of_file_lines = input.collect()
    Rdd_array_of_file_lines.foreach(println)
    scala.io.StdIn.readLine()

}
