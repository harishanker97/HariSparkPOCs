val a = Array(1,2,3,8,7,6,9)
def fun1(a:Int) :Int = 2*a + 2
def func2(a:Int): Boolean = a>4
a.map(fun1)
//a.map(x=>2*x+2)
val d = Array("Hari Shanker ","Reddy Resu")
a.filter(func2)
//a.filter(x=> x > 4)
//d.map(x=>x.toLowerCase()).flatten
d.flatMap(x=>x.toUpperCase)
d.flatten