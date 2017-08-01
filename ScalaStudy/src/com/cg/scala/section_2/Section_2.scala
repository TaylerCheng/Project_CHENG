package com.cg.scala.section_2

import java.util

import scala.collection.immutable.HashMap
import scala.collection.mutable

/**
  * 第二节Scala基本类型及操作、程序控制结构
  */
object Section_2 {

  def main(args: Array[String]): Unit = {
    var i: Int = 5
    var j = new Integer(2)
    i = j
    println(i)
//    val d: Double = 0.0

    val x="Hello"
    val y="Hello"
    //Scala中的对象比较不同于Java中的对象比较
    //Scala基于内容比较，而java中比较的是引用，进行内容比较时须定义比较方法
    println(x==y)

//    var line = ""
//    // 在Scala中不能这么用，因为Scala中的赋值操作返回的值是Unit，而""是String类型，不能进行比较，这是函数式编程语言特有的特点
//    while ((line = readLine()) != "")
//      println("Read: "+ line)

    //for循环的使用
//    val filesHere = (new java.io.File(".")).listFiles
//    for (file <- filesHere)
//       println(file)

    val numsSet=mutable.Set(3.0,5)

    var studentInfo=Map("john" -> 21, "stephen" -> 22,"lucy" -> 20)
    studentInfo.foreach(e=> println(e._1+":"+e._2))
    var john = studentInfo.get("john")
    println(john.get)

    // 元组类型
//    val t = ("john" -> 21)
//    println(t.getClass)

  }

  def gcdLoop(x: Long, y: Long): Long = {
    var a = x
    var b = y
    while (a != 0) {
      val temp = a
      a = b % a
      b = temp
    }
    b
  }

}


