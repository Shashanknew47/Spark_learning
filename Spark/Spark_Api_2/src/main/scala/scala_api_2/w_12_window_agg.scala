package scala_api_2
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window

object w_12_window_agg extends App{

    Logger.getLogger("org").setLevel(Level.ERROR)
    val my_conf = new SparkConf()
    my_conf.set("spark.app.name","my_windows")
    my_conf.set("spark.master","local[*]")

    val spark = SparkSession.builder.config(my_conf).enableHiveSupport().getOrCreate()
    val file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/windowdata.csv"

    val df = spark.read.format("csv").option("inferSchema",true).option("path",file_path).load()

    val header_df = df.toDF("Country","weeknum","numinvoices","total_qauntity","invoicevalue")
    /*
      To solve window function queries. We need 3 things to take care of:
      => partition column - country
      => ordering column - weeknum
      => window size - from 1st row to current row
     */

      val my_window = Window.partitionBy("country").orderBy("weeknum")
        .rowsBetween(Window.unboundedPreceding,Window.currentRow)

    val mydf = header_df.withColumn("running_total",functions.sum("invoicevalue")
      .over(my_window))

      mydf.show()

     // Using Spark SQL
     header_df.createOrReplaceTempView("country_invoice")
     spark.sql("SELECT *, sum(invoicevalue) over(partition by country order by weeknum rows between unbounded preceding and current row) as r FROM country_invoice").show()

}
