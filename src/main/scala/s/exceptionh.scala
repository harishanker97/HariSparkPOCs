package s

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object exceptionh {
  def main(args: Array[String]) {
    def divi(a:Int,b:Int):Double = a/b
   try
     {
       val res = divi(args(0).toInt,args(1).toInt)
       println(res)
     }
    catch
      {
        case exception: Exception =>println("divide by zero exception")
      }
    finally
      {
        println("final block")
      }

  }
  def fun(a:Int,b:Int): Option[Int]
  = {
    if(a==b) Option(a) else null
    }

}