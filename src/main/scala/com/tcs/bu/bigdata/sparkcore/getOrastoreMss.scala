package com.tcs.bu.bigdata.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object getOrastoreMss {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("getOrastoreMss").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    import importAll._

    val df = spark.read.jdbc(ourl,"EMP",oprop)

    df.createOrReplaceTempView("EMP")

    val res = spark.sql("SELECT E.ENAME ename, E.DEPTNO edeptno, M.ENAME mname, M.DEPTNO mdeptno FROM EMP E, EMP M WHERE E.MGR = M.EMPNO")

    res.show()
    res.write.jdbc(msurl,"msemp",msprop)

    spark.stop()
  }
}