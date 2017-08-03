package com.cg.scala.section_28

import java.util.ArrayList;
import scala.collection.JavaConversions._

/**
  * 第一节 Scala语言初步
  */
object Section_28{

  def getList = {
    val list = new ArrayList[String]()
    list.add("摇摆少年梦")
    list.add("学途无忧网金牌讲师")
    list
  }

  def main(args: Array[String]) {
    val list = getList
    list.foreach(println)
    println(list.getClass) //这里还是原类型

    val list2 = list.map(x => x * 2)
    println(list2)

    //显式地进行转换
    val listStr = asJavaIterable(new ArrayList[String]())
    println(listStr.getClass)
    for (i <- listStr)
      println(i)

  }

}