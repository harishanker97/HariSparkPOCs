package com.tcs.bu.bigdata.sparkcore

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import com.tcs.bu.bigdata.sparkcore.importAll._
/*
get data from oracle, and mysql and join these two tables next store in mssql
com.tcs.bu.bigdata.sparkcore.getOracleMysqlData
 */
object getOracleMysqlData {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").enableHiveSupport().appName("getOracleMysqlData").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
val tab = args(0)
    import spark.implicits._
    import spark.sql
    val df = spark.read.jdbc(ourl,"EMP",oprop)
    df.show()

    val mdf = spark.read.jdbc(murl,"DEPT",mprop)
    mdf.show()
    // processing
    df.createOrReplaceTempView("emp")
    mdf.createOrReplaceTempView("dept")
    val res = spark.sql("select e.empno,e.ename,e.job,e.sal, d.loc from emp e join dept d on e.deptno=d.deptno")
    res.show()
res.write.mode(SaveMode.Overwrite).jdbc(msurl,tab,msprop)
    res.write.mode(SaveMode.Overwrite).format("hive").saveAsTable(tab)
    res.write.mode(SaveMode.Overwrite).format("csv").option("header","true").save(s"s3://harishankerbucket2020/output/$tab")
// if u get Exception in thread "main" java.lang.ClassNotFoundException: oracle.jdbc.OracleDriver, its dependency problem
    spark.stop()
  }
}