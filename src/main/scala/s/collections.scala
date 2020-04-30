package s

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
/* collections----1)seq--2)set--3)map
    seq-----list(linear seq)---Array(indexed seq)---vector
 */
object collections {
  def main(args: Array[String]) {
    val a = Array(1,12,14,16,"ss")
    val b = Vector(1,2,3)
    println(b(1))





  }
}