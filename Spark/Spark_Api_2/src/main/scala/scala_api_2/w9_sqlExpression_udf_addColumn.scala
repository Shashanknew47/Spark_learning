package scala_api_2

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object w9_sqlExpression_udf_addColumn extends App{

  def age_check(age: Int) = {
    if (age > 18) "Y" else "N"
  }

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","sql_expression_udf")
    sparkconf.set("spark.master","local[*]")

    val spark =  SparkSession.builder().config(sparkconf).getOrCreate()

    val file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/dataset1"

    val df = spark.read.format("csv")
      .option("inferSchema",true)
      .option("path",file_path)
      .load()

    val df2  = df.toDF("name","age","city")
    // This way(spark.udf.register) function will be registered udf catalog.
    // so, we can use it sql like style also
    spark.udf.register("parseAgeFunction",age_check(_:Int):String)

   /* For udf registration we can write lambda function too ex.
    spark.udf.register("parseAgeFunction",(x:Int) => {if (x > 18) "y" else "n"})
    val df3 = df2.withColumn("adult",expr("parseAgeFunction(age)"))
   df3.show()
    */

     df2.createOrReplaceTempView("peopleTable")

     spark.sql("select name,age,city,parseAgeFunction(age) as isAdult FROM peopleTable").show()

    spark.stop()



}
