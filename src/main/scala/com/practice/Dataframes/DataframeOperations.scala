package com.practice.Dataframes

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by vdokku on 7/6/2017.
  */
object DataframeOperations {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")

    val conf = new SparkConf().setAppName("<< Spark Calculate percentage >>").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._
    val sampleDataframe = Seq(("1", "Alice", "false", "60", "125"),
      ("2", "Brian", "true", "64", "131"),
      ("3", "Charlie", "true", "74", "183"),
      ("4", "Doris", "false", "58", "102"),
      ("5", "Ellen", "false", "66", "140"),
      ("6", "Frank", "true", "66", "151"),
      ("7", "Gerard", "true", "68", "190"),
      ("8", "Harold", "true", "61", "128")).toDF("id", "name", "is_male", "height_in", "weight_lb")

    /*
    +---+-------+-------+---------+---------+
| id|   name|is_male|height_in|weight_lb|
+---+-------+-------+---------+---------+
|  1|  Alice|  false|       60|      125|
|  2|  Brian|   true|       64|      131|
|  3|Charlie|   true|       74|      183|
|  4|  Doris|  false|       58|      102|
|  5|  Ellen|  false|       66|      140|
|  6|  Frank|   true|       66|      151|
|  7| Gerard|   true|       68|      190|
|  8| Harold|   true|       61|      128|
+---+-------+-------+---------+---------+

     */

    sampleDataframe.show()

    // Performing some more operations.


    println("Filtering the table to just show the males.")
    /*
    +---+-----+-------+---------+---------+
| id| name|is_male|height_in|weight_lb|
+---+-----+-------+---------+---------+
|  5|Ellen|  false|       66|      140|
|  6|Frank|   true|       66|      151|
+---+-----+-------+---------+---------+
     */


        sampleDataframe.filter(sampleDataframe("height_in") === "66").show()

        sampleDataframe.filter("height_in = '66'").show()

    sampleDataframe.registerTempTable("sampleData")


    println("<<<<<<<<<<<<<<<<<<<<<<<<<USING the Spark SQL>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    sqlContext.sql("select * from sampleData where height_in = '66'").show()

    // df.filter($"state" === "TX") or df.filter("state = 'TX'") for equality

  }
}
