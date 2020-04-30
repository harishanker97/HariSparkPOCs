val add = "hyd"
val res = add match
{ case "sec" => "secunderbad"
  case "hyd" => "hyderabad"
  case "edu" => "edulabad"
}
def pl(a :String):String =
  a match {
    case "sec" =>( "secunderbad")
    case "hyd" => "hyderabad"
    case "edu" => "edulabad"
  }
print(pl("hyd"))
//recursive func
def fact(a: Int): Int = a match
{
  case a if(a<=1) => 1
  case _ => a * fact(a-1)
}
print(fact(4))
def pow(x:Int,y:Int):Int = y match
  {
  case y if(y<1) =>  1
  case _ =>   x * pow(x,y-1)
}
pow(2,1)
//nested function
def maximum(x:Int,y:Int,z:Int):Int=
  {
    def max(x:Int,y:Int):Int=
      if(x>y) x else y
    max(x,max(z,y))
  }
print(maximum(2,9,6))
//anonymous functions
val c = (a:Int,b:Int) => 2*a*b + a*a + b*b
c(3,2)
//string interpolation
val f = "hi "
val d = s" $f hari shanker reddy resu"
def offer(x:Int):String = x match
{
  case x if(x<5000) => s"you saved ${ x*10/100 } rupess and your bill is ${x-x*10/100}"
  case x if(x>5000 && x<10000) => s"you saved ${ x*20/100 } rupess and your bill is ${x-x*20/100}"
  case x if(x>10000&& x<20000) => s"you saved ${ x*30/100 } rupess and your bill is ${x-x*30/100}"
  case x if(x>20000)=> s"you saved ${ x*40/100 } rupess and your bill is ${x-x*40/100}"
}
offer(6000)
val t = Array(1,2,3,4,4)
val q = Seq(("abc",123),("fgg",124),("hhh",345))
val z = for(x <- t if(x>2)) yield t


