package com.practice.Dataframes

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * Created by vdokku on 7/6/2017.
  */
object CalculatePercentageOfTotal {


  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")

    val conf = new SparkConf().setAppName("<< Spark Calculate percentage >>").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    val df = sc.parallelize(Seq(
      ("A", 0), ("A", 1), ("A", 0),
      ("B", 0), ("B", 1), ("B", 1)
    )).toDF("Name", "Flag")


    df.show()
    /*

Now the data looks like this

+----+----+
17/07/06 11:09:59 INFO SparkContext: Invoking stop() from shutdown hook
|Name|Flag|
+----+----+
|   A|   0|
|   A|   1|
|   A|   0|
|   B|   0|
|   B|   1|
|   B|   1|
+----+----+


    I'd like to transform it to:

Name | Total | With Flag | Percentage
A    | 3     | 1         | 33%
B    | 3     | 2         | 66%


     */

    // Name, Total count, total count of 1's, PERCENTAGE.

    df.registerTempTable("FlagTempTable")

    sqlContext.sql("select Name, count(*) Total, SUM(Flag) With_Flag, CAST(AVG(Flag) * 100 AS INT) percentage from FlagTempTable group by Name ").show()
  }

}
