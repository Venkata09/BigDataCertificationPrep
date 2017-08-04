package com.practice.Dataframes

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by vdokku on 7/7/2017.
  */
object ExplodeDemo {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")

    val conf = new SparkConf().setAppName("<< Spark Loading of CSV >>").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)


    import sqlContext.implicits._
    val sampleDataframe = sc.parallelize(Seq((1, Seq(2, 3, 4), Seq(5, 6, 7)), (2, Seq(3, 4, 5), Seq(6, 7, 8)), (3, Seq(4, 5, 6), Seq(7, 8, 9)))).toDF("a", "b", "c")

    sampleDataframe.show()

    /*
    +---+---------+---------+
|  a|        b|        c|
+---+---------+---------+
|  1|[2, 3, 4]|[5, 6, 7]|
|  2|[3, 4, 5]|[6, 7, 8]|
|  3|[4, 5, 6]|[7, 8, 9]|
+---+---------+---------+

It's like a sequence of squences, so we need to explode inorder to see all the values.



     */

    import org.apache.spark.sql.functions._
    val df1 = sampleDataframe.select(sampleDataframe("a"), explode(sampleDataframe("b")).alias("b_columns"), sampleDataframe("c"))

    val df2 = df1.select(df1("a"), df1("b_columns"), explode(df1("c").alias("c_columns")))


    sampleDataframe.show()
    df1.show()
    df2.show()


  }
}
