package com.tcs.bu.bigdata.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object mar27 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("mar27").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val in = "C:\\work\\dataset\\namedonations.txt"
    val rdddata = sc.textFile(in)
    val res = rdddata.map(x=>x.split(',')).map(x=>(x(0),x(1))).sortBy(x=>x._1)
     res.collect().foreach(println)
    

    spark.stop()
  }
}