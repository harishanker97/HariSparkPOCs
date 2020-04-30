package com.tcs.bu.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object oscar {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("oscar").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val path = "C:\\work\\dataset\\Oscars.txt"
/*   val df = spark.read.option("header",true).option("inferschema",true).load(path)
   // df.createOrReplaceTempView("osc")
   // val res = spark.sql(" select * from osc")
    //res.show()
   // res.printSchema()
*/
    val rd1 = sc.textFile(path).map(x=> x.split("\t"))
    //val d = rd1.toDF()
    //d.createOrReplaceTempView("tab")
    //val r =spark.sql("select value[0], value[2] from tab")
    //r.show()
    //r.printSchema()

    spark.stop()
  }
}