/*
complex json to simple json data
-> if any '$' symbol available in column name then use " `$colname` " below esc button
-> for array use a[1],a[2].
-> for struct use "S_parent.S_child"
-> for struct in arr use explode.

 */

package com.tcs.bu.bigdata.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object compjson {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("compjson").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val data = "C:\\work\\dataset\\world_bank.json"
    val df =spark.read.format("json").load(data)

    df.createOrReplaceTempView("wb")
    val query="""select  _id.`$oid` OID, approvalfy,board_approval_month,boardapprovaldate, borrower,closingdate,country_namecode,countrycode, supplementprojectflg,countryname,countryshortname,envassesmentcategorycode,grantamt,ibrdcommamt,id,idacommamt,impagency,lendinginstr, lendinginstrtype,lendprojectcost, mjthemecode,sector.name[0] name1,sector.name[1] name2, sector.name[2] name3,sector1.name S1Name, sector1.percent S1Percent,sector2.name S2Name, sector2.percent S2Percent,sector3.name S3Name, sector3.percent S3Percent,sector4.name S4Name, sector4.percent S4Percent, prodline, prodlinetext, productlinetype,mjtheme[0] mjtheme1,mjtheme[1] mjtheme2,mjtheme[2] mjtheme3  , project_name ,projectfinancialtype, projectstatusdisplay,regionname,sectorcode, source,status,themecode,totalamt,totalcommamt,url,mp.Name mpname, mp.Percent mppercent, tn.code tncode, tn.name tnname,mn.code mncode, mn.name mnname, tc.code tccode, tc.name tcname, pd.DocDate pddocdate ,pd.DocType pddoctype, pd.DocTypeDesc pddoctypedesc, pd.DocURL  pddocurl, pd.EntityID pdid from wb lateral view explode(majorsector_percent) tmp as mp lateral view explode(theme_namecode) tmp as tn lateral view explode(mjsector_namecode) tmp as mn lateral view explode(mjtheme_namecode) tmp as tc lateral view explode(projectdocs) tmp as pd"""
    val res =spark.sql(query).cache()
    res.createOrReplaceTempView("new")
    res.show()
    df.printSchema()
    res.printSchema()
    spark.stop()
  }
}