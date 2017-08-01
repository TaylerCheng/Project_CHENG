package com.cg.scala.section_21

/**
  * 1. 中置类型(Infix Type)
  */
object Section_23 {
  def main(args: Array[String]): Unit = {
    var subList:MyList[B] = new MyList[B]()
    var superList:MyList[A] = subList

    //Liskov代替原理: 它规定T类型是U类型的子类条件是，在U对象出现的所有地方都可以用T对象来代替
    superList.add(new B)
    subList.add(new B)

    superList.add(new A)
    subList.add(new A)

    superList.test(new B)

  }
}

//定义自己的List类
class MyList[+T](){

  def add[U >: T](element: U): Unit = {
    println(element.toString)
  }

  def test(element: B): Unit = {
    println(element.toString)
  }

}

class A

class B extends A

class C extends A

