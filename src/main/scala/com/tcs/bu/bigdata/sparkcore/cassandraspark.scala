package com.tcs.bu.bigdata.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object cassandraspark {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("cassandraspark").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val df = spark.read.format("org.apache.spark.sql.cassandra").option("table", "Emp_record").option("keyspace","venuks").load()
    val df1 =spark.read.format("org.apache.spark.sql.cassandra").option("table", "dept").option("keyspace","venuks").load()
    df.createOrReplaceTempView("emp_record")
    df1.createOrReplaceTempView("dept")

    val res = spark.sql("select d.*,e.* from emp_record e join dept d on d.e_id = e.e_id ")
    res.show()
    spark.stop()
  }
}