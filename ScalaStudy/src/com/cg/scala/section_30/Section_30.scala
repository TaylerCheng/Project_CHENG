package com.cg.scala.section_30

/**
  * 第三十节 Scala数据库编程
  */
object Section_30 {

  println("hello")
  print(toString())

  override def toString() = "firstName:"

  def main(args: Array[String]): Unit = {

  }
}

class Person(val firstName: String, val secondName: String) {
  override def toString() = "firstName:" + firstName + ",secondName:" + secondName
}
