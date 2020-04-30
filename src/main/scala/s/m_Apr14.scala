package s

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import func.{narg, _}

object m_Apr14 {
  def main(args: Array[String]) {
    val res = add(4,5)
    println(res)
    println(mul(4,4))
    println(sub(16,4))
    println(div(48,4))
    println(modu(23,8))
    println("-----------------------")
    println(fact(5))
    println("-----------------------")
    val c = narg(narg = 1,2,3,4,5)
    println(c(1))
    println("-----------------------")
    println(af(4,2))
    println("-----------------------")
    println(hof(4,5,mul))
    println(hof(5,2,div))
    val q = hof1(5)
    println(q(2))
    println("-----------------------")
    val e = paf(6,_:Int,_:Int)
    val f = e(7,_:Int)
    //val g = f _                                             //passing no parameter
    println(f(8))
    println("-----------------------")
    val a = pg(1)(2,3)_
    println(a(4))
    println("-----------------------")
    println(clo(6.6))
      }

  def add(a:Int,b:Int):Int = a+b
}

object func{
  def mul(a:Int,b:Int):Int = { a*b  }
  def sub(a:Int,b:Int):Int = { a-b  }
  def div(a:Int,b:Int):Int = { a/b  }
  def modu(a:Int,b:Int):Int = { a%b  }
  def fact(a:Int):BigInt = {                                //recursive func
    if (a==1 || a==0) 1 else a*fact(a-1)
  }
  def narg(narg :Int*) = narg                             //variable parameter func
  val af = (a:Int,b:Int)=> a/b                                      //anonymous function
  def hof(a:Int,b:Int,f:(Int,Int)=>Int):Int = f(a,b)                //higher order function takes function as parameter
  def hof1(a:Int) = (m:Int)=> m*a                       //higher order function can return function
  def paf(a:Int,b:Int,c:Int):Int = a+b+c                            //partially applied function defined above
  def pg(a:Int)(b:Int,c:Int)(d:Double):Int                          //parameter grouping / currying
  = a*b*c*(d.toInt)
  val x=10
  def clo(a:Double):Int = (a*x).toInt                               //closure fun (impure func) (uses global values)
}

