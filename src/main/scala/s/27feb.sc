val a = "hari"*3+"shanker"
s"hari ${a*3}"
val b = 6 < 5


def sum(x:Int, y:Int) = x+y

sum(3,8)
//function: hide logic and re-usable
//recurive functin
//a function call itself called recursive. return datatype mandatory
//factorial

//6 fact means 6*5*4*3*2*1
def fact(x:Int):Int = x match {
  case x if(x<=1)=> 1
  case _ => x* fact(x-1)
}
fact(4)
//4* FACT(3)
//4*3*2*1

//power
//2 pow 5 ... 2 is base, 5 is power
2*2*2*2*2 //32
def power(b:Int,p:Int):Int = p match {
  case p if(p<1) => 1
  case _ => b * power(b,p-1)
}
// 2 * power(2,4)
//2 * 2* power(2,3)
// 2 * 2 * 2 * power(2,2)
// 2 * 2*2* 2* power(2,1)
// 2 * 2*2*2*2 * 1

power(2,5)
//2*2*2 * power(2,2)

//nested function
// a function call in another function.
def maxnum(x:Int, y:Int, z:Int) = {
  def max(a:Int, b:Int)= if(a>b) a else b
  max(x,max(y,z))
}
maxnum(2,6,9) //9

//anonymous function
// a function without function name and def ... called
//its almost like lambda in python
// => means do something action.
val res = (fn:String, ln:String) => fn+ " " + ln
//def testing (fn:String, ln:String) = fn+ " " + ln
res("venu","apositive")

// higher order function
// a fimctopm ca;; as parameter in another function called hof

def sqr(x:Int) = x * x
def cub(x:Int) = x*x*x
fact(5)

def multi(x:Int, y:Int) = x*y
def sums(x:Int, y:Int) = x+y


def hof(f:Int=>Int, x:Int) = {
  f(x)
}

hof(sqr, 4)
hof(cub,4)
hof(fact,4)
def higher(f:(Int, Int)=>Int, x:Int, y:Int) = {
  f(x,y)
}

higher(power, 5,4)