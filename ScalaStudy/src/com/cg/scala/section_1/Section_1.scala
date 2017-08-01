package com.cg.scala.section_1

/**
  * 第一节 Scala语言初步
  */
object Section_1 {
  def main(args: Array[String]): Unit = {

    val helloString: String = "Hello World" //val类型相当于final，不能重新赋值
    var helloString2: String = "Hello World" //var类型可以重新赋值
    helloString2 = "Hello Crazy World"
  }
}