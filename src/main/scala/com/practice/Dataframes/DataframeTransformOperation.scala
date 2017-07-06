package com.practice.Dataframes

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
  *
  *
  * This functions is to transform a single column name & it's values.
  *
  * For this we wrote a UDF and read all the column names & applied the UDF to transform the values.
  *
  * Created by vdokku on 7/6/2017.
  */
object DataframeTransformOperation {

  private case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")

    val conf = new SparkConf().setAppName("<< Spark Transform certain columns & values >>").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    // create an RDD with some data
    val custs = Seq(
      Cust(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Cust(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Cust(3, "Widgetry", 410500.00, 200.00, "CA"),
      Cust(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )
    val customerDF = sc.parallelize(custs, 4).toDF()

    // the original DataFrame
    customerDF.show()

    // UDF is derived from the SQL Functions.
    val myFunc = udf {(x: Double) => x + 1}

    // get the columns, having applied the UDF to the "discount" column and leaving the others as they were
    val colNames = customerDF.columns
    val cols = colNames.map(cName => customerDF.col(cName))
    val theColumn = customerDF("discount")
    val mappedCols = cols.map(c => if (c.toString() == theColumn.toString()) myFunc(c).as("transformed") else c)
    mappedCols.foreach(eachCol => println(eachCol))
    // use select() to produce the new DataFrame
    val newDF = customerDF.select(mappedCols:_*)
    newDF.show()
  }
}
