package com.tcs.bu.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object json {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("json").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val path = "C:\\work\\dataset\\zips.json"

    val df = spark.read.format("json").option("inferSchema","true").load(path)

   // val resr = df.rdd
    // resr.saveAsTextFile("C:\\work\\dataset\\output\\ss\\mm")

df.createOrReplaceTempView("tab")
    val res = spark.sql("SELECT CAST(_id AS INT) ID,city City, loc[0] Lon, loc[1] lat ,pop Pop, case when state='MA' then 'MAA' end state from tab")
    val res1 = res.withColumn(colName = "age", lit(18))
    res1.show()
    res1.printSchema()

    spark.stop()
  }
}