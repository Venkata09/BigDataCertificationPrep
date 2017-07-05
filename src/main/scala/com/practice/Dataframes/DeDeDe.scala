package com.practice.Dataframes

import org.apache.spark.{SparkConf, SparkContext}

/**
  * De-Duplication in Dataframe
  * Created by vdokku on 7/2/2017.
  */
object DeDeDe {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")

    val conf = new SparkConf().setAppName("<< Spark Loading of CSV >>").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)


    import sqlContext.implicits._

    /*

    1, 6 -> Name is SAME.
    ID's are same for couple of id's

     */

    val custs = Seq(
      (1, "Widget Co", 120000.00, 0.00, "AZ"),
      (2, "Acme Widgets", 410500.00, 500.00, "CA"),
      (3, "Widgetry", 410500.00, 200.00, "CA"),
      (4, "Widgets R Us", 410500.00, 0.0, "CA"),
      (3, "Widgetry", 410500.00, 200.00, "CA"),
      (5, "Ye Olde Widgete", 500.00, 0.0, "MA"),
      (6, "Widget Co", 12000.00, 10.00, "AZ")
    )
    val customerRows = sc.parallelize(custs)

    // convert RDD of tuples to DataFrame by supplying column names
    val customerDF = customerRows.toDF("id", "name", "sales", "discount", "state")

    customerDF.show()


  }
}
