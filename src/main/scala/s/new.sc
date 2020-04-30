val x = 36
val ar = Array(1,2,3,4,5,6)
ar(0) = 9
ar
val ar1= Array("hari",36:Byte)
val ar2= Array('a','A','B','b',1)
val li = List(5,6,7,8)
// list is immutable
val seq1 = Seq(9,10,11,12)
//sequence is implementing list, and its is also immutable
//sequence is used with tuple
val ar3 = Array( li,seq1,ar1)
val ar4 = Array("hari",24,"hyd")
val t1 = ("hari",24,"hyd")
// to collect diff datatypes--> use tuples
if(t1._2 > 25) "above 25" else "below 25"
val sq2 = Seq(("hari",99,true),("shiva",88,true),("xxx",'a',false))
val res= {
  val x = 12
  val y = 3
  x / y
}
// the above is block ,last statement is return statement,
// assigned to res variable
def fun1(a:Int):Int  =
  a*a
fun1(3)