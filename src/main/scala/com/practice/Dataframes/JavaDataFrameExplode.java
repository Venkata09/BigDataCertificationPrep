package com.practice.Dataframes;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.Arrays;
import java.util.List;

/**
 * Created by vdokku on 7/6/2017.
 */
public class JavaDataFrameExplode {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\");

        SparkConf conf = new SparkConf().setAppName("").setMaster("local[1]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        String jsonData = "{\"id\":4,\"score\":358,\"viewCount\":24247,\"tags\":[\"c#\",\"winforms\",\"type-conversion\",\"opacity\"]}";

        List dataSet = Arrays.asList(jsonData);

        JavaRDD distData = sc.parallelize(dataSet);

        DataFrame stackoverflow_Posts = sqlContext.read().json(distData);

        stackoverflow_Posts.printSchema(); //let's print out the DataFrame schema (Output#1)

        stackoverflow_Posts.show(); //let's show the DataFrame content (Ouput#2)

        DataFrame expanded = stackoverflow_Posts.withColumn("tag", org.apache.spark.sql.functions.explode(stackoverflow_Posts.col("tags")));

        expanded.printSchema(); //let's print out the DataFrame schema again (Output#3)

        expanded.show(); //let's show the DataFrame content (Output#4)


        String jsonData1 = "{\"id\":4,\"score\":358,\"viewCount\":24247,\"tags\":[\"c#\",\"winforms\",\"type-conversion\",\"opacity\"]}";

    }
}
