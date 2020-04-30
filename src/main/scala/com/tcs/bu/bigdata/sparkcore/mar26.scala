package com.tcs.bu.bigdata.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object mar26 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("mar26").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "C:\\work\\dataset\\us-500.csv"
    //val data = args(0)
    val usrdd = sc.textFile(data)
    val skip = usrdd.first()
  //val reg = ",(?=([^"]"[^"]")[^"]$)"
    val reg = ","
    // select state, count(*) cnt from tab group by state order by cnt desc
    val res = usrdd.filter(x=>x!=skip).map(x=>x.split(reg)).map(x=>(x(6).replaceAll("\"",""),1)).reduceByKey((a,b)=>a+b).sortBy(x=>x._2,false)
    res.take(50).foreach(println)
    spark.stop()
  }
}