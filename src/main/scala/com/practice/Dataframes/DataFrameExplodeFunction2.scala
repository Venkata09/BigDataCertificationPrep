package com.practice.Dataframes

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * https://stackoverflow.com/questions/39115640/spark-dataframe-explode-function
  * Created by vdokku on 7/6/2017.
  */
object DataFrameExplodeFunction2 {

  def main(args: Array[String]): Unit = {


    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")
    import org.apache.spark.sql.Row
    val conf = new SparkConf().setAppName("<< Spark Calculate percentage >>").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._
    val dfExplode = sc.parallelize(Array((1, "dsfds dsf dasf dsf dsf d"), (2, "2344 2353 24 23432 234"))).toDF("id", "text")

    dfExplode.map { case row: Row => (row(0), row(1)) }
    dfExplode.map { case row: Row => (row(0).asInstanceOf[Int], row(1).asInstanceOf[String]) }
    dfExplode.show()


    // TODO: Need to find a better way.

    /*import org.apache.spark.sql.Row
    case class ExplodedData(word: String)
    dfExplode.explode("id", "text") { case row: Row =>
      val words = row[
      1].asInstanceOf[String].split(" ")
      words.map(word => ExplodedData(word))
    }*/

  }
}
