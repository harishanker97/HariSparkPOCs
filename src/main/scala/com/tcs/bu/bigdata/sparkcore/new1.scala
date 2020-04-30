package com.tcs.bu.bigdata.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object new1 {
  def main(args: Array[String]) {
    // sparksession used to create dataset
    val spark = SparkSession.builder.master("local[*]").appName("hello").getOrCreate() //dataframe/dataset
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext //to create rdd

    import spark.implicits._
    import spark.sql
    //val data = Array("venu","Mani","Geetanshu","naveen Kumar")
    //java/scala object convert to rdd
    //  val rdd = sc.parallelize(data)
    //second way to create rdd sc.textfile
/*val output = "C:\\work\\dataset\\output\\asldata"
    val path = "C:\\work\\dataset\\asl.txt"
    this is called hardcoding its not recommended.*/
    val path = args(0)
    val output = args(1)
    // data convert to rdd
    val aslrdd = sc.textFile(path)
    //select * from tab where city='del'
    //val res = aslrdd.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2))).filter(x=>x._3=="del")
    // select city, count(*) cnt from tab group by city order by cnt desc
    //90% dataset/dataframe 99% dataset
    //val res = aslrdd.map(x=>x.split(",")).map(x=>(x(2),1)).reduceByKey((a,b)=>a+b).sortBy(x=>x._2,false)
    // 2 type info : header, original data, header: representation of original data., original data: usually used data,
    // in hadoop point of view no diff between header/schema/original data.
    val fil = aslrdd.first()
    //val res = aslrdd.filter(x=>x!=fil).map(x=>x.split(",")).map(x=>(x(0),x(1).toInt,x(2))) .filter(x=>x._3=="mas" && x._2>40) //Array("satya","33","mas")
    val res = aslrdd.filter(x=>x!=fil).map(x=>x.split(",")).map(x=>(x(0),x(1).toInt,x(2))) .filter(x=> x._2>40)

    // here array data convert to Tuple ..
    res.collect.foreach(println)
    res.saveAsTextFile(output)


    spark.stop()
  }
}