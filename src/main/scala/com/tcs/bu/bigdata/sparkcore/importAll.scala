package com.tcs.bu.bigdata.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object importAll {

  val msurl="jdbc:sqlserver://venudb.cnwu9xc3aep7.ap-south-1.rds.amazonaws.com:1433;databaseName=venutasks;"
  val msprop = new java.util.Properties()
  msprop.put("user","msusername")
  msprop.put("password","mspassword")
  msprop.put("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")


  val murl="jdbc:mysql://dbmysql.ckyod3xssluv.ap-south-1.rds.amazonaws.com:3306/dbmysql"
  val mprop = new java.util.Properties()
  mprop.put("user","musername")
  mprop.put("password","mpassword")
  mprop.put("driver","com.mysql.cj.jdbc.Driver")

  val ourl="jdbc:oracle:thin:@//mydb.c2dj1vzohp5n.ap-south-1.rds.amazonaws.com:1521/ORCL"
  val oprop = new java.util.Properties()
  oprop.put("user","ousername")
  oprop.put("password","opassword")
  oprop.put("driver","oracle.jdbc.OracleDriver")
}