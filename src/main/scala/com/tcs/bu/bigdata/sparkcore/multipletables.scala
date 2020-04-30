package com.tcs.bu.bigdata.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.tcs.bu.bigdata.sparkcore.importAll._
object multipletables {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("multipletables").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    //val tabs = Array("EMP","DEPT")
    /*   tabs.foreach { x =>
     val tab = x.toString()
     val df = spark.read.jdbc(ourl, s"$tab", oprop)
     df.write.jdbc(msurl,s"hari$tab",msprop)
     df.show()
   }
*/
    val qur = "(SELECT E.ENAME empl, M.ENAME man, E.DEPTNO, E.SAL FROM emp E,emp M WHERE E.MGR = M.EMPNO) h"

      val df = spark.read.jdbc(ourl, s"$qur", oprop)
      df.write.jdbc(msurl,"hariselfjoin",msprop)
      df.show()

    spark.stop()
  }
}