package com.practice.Dataframes

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by vdokku on 7/1/2017.
  */
object Dataframe_Basics {


  case class Customer(id: Integer, name: String, sales: Double, discount: Double, state: String)


  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")

    val conf = new SparkConf().setAppName("<< Spark Loading of CSV >>").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)


    import sqlContext.implicits._

    val custs = Seq(
      Customer(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Customer(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Customer(3, "Widgetry", 410500.00, 200.00, "CA"),
      Customer(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Customer(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )


    val customerDF = sc.parallelize(custs).toDF()

    println("PRINTING CUSTOMER SCHEMA")
    customerDF.printSchema()

    customerDF.show()

    //TODO: LEAD, LAG, Rank based on the salary & State. That kind-of information.

    //    customerDF.select("state").show()

    /*

    +-----+
|state|
+-----+
|   AZ|
|   CA|
|   CA|
|   CA|
|   MA|
+-----+
     */


    //    customerDF.select("id","discount").show()

    /*
    +---+--------+
| id|discount|
+---+--------+
|  1|     0.0|
|  2|   500.0|
|  3|   200.0|
|  4|     0.0|
|  5|     0.0|
+---+--------+
     */


    customerDF.select("*").show()

    // Filter operation.


    customerDF.filter($"state".equalTo("CA")).show()

    customerDF.registerTempTable("CustomerTable")

    sqlContext.sql("select * from CustomerTable where state = 'CA'").show()


    // Modifications to the dataframe, like Dropping few columns in dataframe.

    println("<<<<<<<<<<<<<< BEFORE DROPPING>>>>>>>>>>>>>>>")

    customerDF.show()

    val cutDownColumns = customerDF.drop("sales")

    /*
    +---+---------------+--------+--------+-----+
| id|           name|   sales|discount|state|
+---+---------------+--------+--------+-----+
|  1|      Widget Co|120000.0|     0.0|   AZ|
|  2|   Acme Widgets|410500.0|   500.0|   CA|
|  3|       Widgetry|410500.0|   200.0|   CA|
|  4|   Widgets R Us|410500.0|     0.0|   CA|
|  5|Ye Olde Widgete|   500.0|     0.0|   MA|
+---+---------------+--------+--------+-----+
     */
    println("<<<<<<<<<<<<<< AFTER DROPPING>>>>>>>>>>>>>>>")

    cutDownColumns.show()

    /*
    +---+---------------+--------+-----+
| id|           name|discount|state|
+---+---------------+--------+-----+
|  1|      Widget Co|     0.0|   AZ|
|  2|   Acme Widgets|   500.0|   CA|
|  3|       Widgetry|   200.0|   CA|
|  4|   Widgets R Us|     0.0|   CA|
|  5|Ye Olde Widgete|     0.0|   MA|
+---+---------------+--------+-----+

     */

    /*

    You can define the schema for the data like the below, if you don't want to create a CASE CLASS.

    val customerDF = customerRows.toDF("id", "name", "sales", "discount", "state")

     */



  }
}
