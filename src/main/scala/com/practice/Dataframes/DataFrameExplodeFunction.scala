package com.practice.Dataframes

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by vdokku on 7/6/2017.
  */
object DataFrameExplodeFunction {

  def main(args: Array[String]): Unit = {


    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")

    val conf = new SparkConf().setAppName("<< Spark Calculate percentage >>").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val jsonData = sqlContext.read.json("src/main/resources/inputData/JsonExplodeFunction.json").toDF()
    //    jsonData.show()

    /*
    An Explode function has two definitions

    def explode[A <: scala.Product](input : org.apache.spark.sql.Column*)(f : scala.Function1[org.apache.spark.sql.Row, scala.TraversableOnce[A]])(implicit evidence$2 : scala.reflect.runtime.universe.TypeTag[A]) : org.apache.spark.sql.DataFrame = { /* compiled code */ }
      def explode[A, B](inputColumn : scala.Predef.String, outputColumn : scala.Predef.String)(f : scala.Function1[A, scala.TraversableOnce[B]])(implicit evidence$3 : scala.reflect.runtime.universe.TypeTag[B]) : org.apache.spark.sql.DataFrame = { /* compiled code */ }

     */
    import scala.collection.mutable.ArrayBuffer
    val elementsRdd = jsonData.select(jsonData("r")).map(t => t.getAs[ArrayBuffer[Long]](0)).flatMap(x => x)
    elementsRdd.count()
    elementsRdd.take(15)


    import scala.collection.mutable.ArrayBuffer
    val jj1 = jsonData.explode("r", "r1") { list: ArrayBuffer[Long] => list.toList }
    val jj2 = jj1.select("r1")
    jj2.collect


  }
}
