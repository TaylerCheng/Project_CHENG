package com.cg.scala.section_23

/**
  * 1. 中置类型(Infix Type)
  */
object Section_23 {
  def main(args: Array[String]): Unit = {

  }
}


//定义Person类，两个泛型参数，分别是S，T,因此
//它是可以用中置表达式进行变量定义的
case class Person[S,T](val name:S,val age:T)

object InfixType extends App {
  //下面的代码是一种中置表达方法，相当于
  //val p:Person[String,Int]
  val p:String Person Int= Person("摇摆少年梦",18)

  //中置表达式的模式匹配用法
  //模式匹配时可以直接用常量，也可以直接用变量
  p match {
    case "摇摆少年梦" Person 18=> println("matching is ok")
    case name Person age=> println("name:"+name+"  age="+age)
  }
}

object ExisitType extends App{

  def print(x:Array[_])=println(x)

  def print2(x:Array[T] forSome {type T})=println(x)

  //Map[_,_]相当于Map[T,U] forSome {type T;type U}
  def print3(x:Map[_,_])=println(x)

  print(Array("摇摆少年梦","学途无忧网金牌讲师"))
  print2(Array("摇摆少年梦","学途无忧网金牌讲师"))
  print3(Map("摇摆少年梦"->"学途无忧网金牌讲师"))

}

//来自API文档中的例子,Function2
object Main extends App {
  //max与anonfun2是等价的，它们定义的都是输入参数是两个Int类型
  //返回值也是Int类型的函数。
  val max = (x: Int, y: Int) => if (x < y) y else x

  //通过Funtion2定义一个输入参数为两个整型
  //返回类型为Int的函数,这里是通过new创建创建函数
  //而这个类正是Function2，它是函数类型类
  val anonfun2 = new Function2[Int, Int, Int] {
    def apply(x: Int, y: Int): Int = if (x < y) y else x
  }
  println(max(0, 1) == anonfun2(0, 1))
}

//4. 抽象类型
//下面定义了一个抽象类
//抽象类中用type关键字声明了一个抽象类型IndentityType
abstract class Person1 {
  type IdentityType

  //方法的返回值类型被声明为抽象类型
  def getIdentityNo(): IdentityType
}

//在子类中，对抽象类型进行具体化
class Student extends Person1 {
  //将抽象类型具体化为String类型
  type IdentityType = String

  def getIdentityNo() = "123"
}

class Teacher extends Person1 {
  //将抽象类型具体化为Int类型
  type IdentityType = Int

  def getIdentityNo() = 123
}

object AbstractType {
  def main(args: Array[String]): Unit = {
    //返回的是String类型
    println(new Student().getIdentityNo())
    //返回的是Int类型
    println(new Teacher().getIdentityNo())
  }

}

//下面是抽象类型的定义方式
trait Closable{
  type in
  type out
  def close(x:in):out
}

class File extends Closable{
  type in=String
  type out=Boolean
  def close(x:in):out= true
  //....其它方法
}

//下面的代码是类型参数的定义方式：
trait Closable2[S,T]{
  def close(x:S):T
}

class File2 extends Closable2[String,Boolean]{
  def close(x:String):Boolean= true
  //....其它方法
}


