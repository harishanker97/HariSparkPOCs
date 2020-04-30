package com.tcs.bu.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UDF {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("UDF").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val path = "C:\\work\\dataset\\bank-full.csv"

    val df = spark.read.format("csv").option("header", "true").option("delimiter", ";").option("inferSchema", "true").load(path)
    df.createOrReplaceTempView("tab")
    val off = udf(offeragebal _)
    spark.udf.register("offeragebal",off)
    //val df1 = spark.sql("SELECT *,offeragebal(age,balance) FROM tab where age > 70")
    df.show()
    df.printSchema()


    spark.stop()
  }
  def offeragebal(age:Int,bal:Int):String
      =(age,bal) match {
    case (a,b) if(a>30 && b>10000)=> " 10% discount"
    case (a,b) if(a>50 && b> 6000)=> " 15% discount"
    case (a,b) if(a>70 && b> 4000)=> " 20% discount"
    case _ => "5% discount"
  }

}