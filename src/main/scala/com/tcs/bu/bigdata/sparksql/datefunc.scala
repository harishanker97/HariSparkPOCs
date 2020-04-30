package com.tcs.bu.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object datefunc {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("datefunc").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val df= spark.createDataFrame(Seq(
      (7369, "SMITH", "CLERK", 7902, "17-Dec-80", 800, 20, 10),
      (7499, "ALLEN", "SALESMAN", 7698, "20-Feb-81", 1600, 300, 30),
      (7521, "WARD", "SALESMAN", 7698, "22-Feb-81", 1250, 500, 30),
      (7566, "JONES", "MANAGER", 7839, "2-Apr-81", 2975, 0, 20),
      (7654, "MARTIN", "SALESMAN", 7698, "28-Sep-81", 1250, 1400, 30),
      (7698, "BLAKE", "MANAGER", 7839, "1-May-81", 2850, 0, 30),
      (7782, "CLARK", "MANAGER", 7839, "9-Jun-81", 2450, 0, 10),
      (7788, "SCOTT", "ANALYST", 7566, "19-Apr-87", 3000, 0, 20),
      (7839, "KING", "PRESIDENT", 0, "17-Nov-81", 5000, 0, 10),
      (7844, "TURNER", "SALESMAN", 7698, "8-Sep-81", 1500, 0, 30),
      (7876, "ADAMS", "CLERK", 7788, "23-May-87", 1100, 0, 20)
    )).toDF("empno", "ename", "job", "mgr", "dob", "sal", "comm", "deptno")

    df.createOrReplaceTempView("emp")
    //val df1 = spark.sql(" select *, current_date(), to_date(dob,'dd-MMM-yy')) dob1 from emp")
    //val df1 = spark.sql(" select *,current_date(),add_months(to_date(dob,'dd-MMM-yy'),100) 100mon from emp")
    //val df1 = spark.sql(" select *,current_date(),date_add(to_date(dob,'dd-MMM-yy'),-100) 100days from emp")
    //val df1 = spark.sql(" select *,current_date(),datediff(current_date(),to_date(dob,'dd-MMM-yy')) daysdf from emp")
    //val df1 = spark.sql(" select *,current_date(),months_between(current_date(),to_date(dob,'dd-MMM-yy')) mondiff from emp")
    //val df1 = spark.sql(" select *,current_date(),year(current_date()) - year(to_date(dob,'dd-MMM-yy')) yearsdiff from emp")
    //val df1 = spark.sql(" select *,current_date(),next_day(current_date(),'WED') nexday from emp")
    //val df1 = spark.sql(" select *, current_date(), quarter(to_date(dob,'dd-MMM-yy')) quat from emp")
    //val df1 = spark.sql(" select *, dayofweek(current_date()) daynum from emp")
    //val df1 = spark.sql(" select *, dayofmonth(current_date()) num_in_mon from emp")
    val df1 = spark.sql(" select *,datediff(current_date(),last_day(current_date())) monlastdaydiff from emp")
    df1.show()

    spark.stop()
  }
}