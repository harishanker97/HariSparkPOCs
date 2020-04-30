package com.tcs.bu.bigdata.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object reduceByKey {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("reduceByKey").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
   // val data = "C:\\work\\dataset\\us-500.csv"
    // val out = "C:\\work\\dataset\\output\\ss"
    val data = args(0)
    val out =  args(1)
    val usrdd = sc.textFile(data)
    val skip = usrdd.first()
    val reg = ","
    val all = "[^a-zA-Z]"
    // val all = "\""
    //alapv4974c, 9847159150
    // val reg = ","
    // select state, count(*) cnt from tab group by state order by cnt desc
    val res = usrdd.filter(x=>x!=skip).map(x=>x.split(reg)).map(x=>(x(6).replaceAll(all,""),1)).reduceByKey((a,b)=>a+b).sortBy(x=>x._2,false)
    //res.take(50).foreach(println)
    res.coalesce(numPartitions = 1)saveAsTextFile(out)

    spark.stop()
  }
}