package com.practice.Dataframes

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by vdokku on 8/4/2017.
  */
object CalculateSumAndCount {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")

    val conf = new SparkConf().setAppName("<< Spark Calculate percentage >>").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._
    val client = Seq((1, "A", 10), (2, "A", 5), (3, "B", 56)).toDF("ID", "Categ", "Amnt")

    /*

    (ENTRIES >>>> ,1,A,10) --> THis make sense becuase it's the ROW.
    ()                     ---> I am not sure about this at all................
    (ENTRIES >>>> ,2,A,5)
    ()
    (ENTRIES >>>> ,3,B,56)
    ()

     */

    val values = client.rdd.zipWithIndex().map(entry => println("ENTRIES >>>> ", entry._1(0), entry._1(1), entry._1(2)))

    /*

For getting the specific rows.
You can specify the index to the
    ([1,A,10],0)      ---> This will make ROW (with the values), LONG (Index value)
    ([2,A,5],1)       --->
    ([3,B,56],2)


You can play around with the map

values = (df.rdd.zipWithIndex()
            .filter(lambda ((l, v), i): i == myIndex)
            .map(lambda ((l,v), i): (l, v))
            .collect())


     */
    values.foreach(println)

    import org.apache.spark.sql.functions._
    client.groupBy("Categ").agg(sum("Amnt").as("Sum"), count("ID").as("count")).show()


  }
}
