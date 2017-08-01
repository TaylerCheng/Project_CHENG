package com.cg.scala.section_3

import scala.collection.mutable.ArrayBuffer

/*
 * 第三节 Array、List
 */
object Section_3 {
  def main(args: Array[String]): Unit = {

    //    val strArray = Array("First", "Second")
    //    val strArray2 = new Array[String](10)
    //    for (item <- strArray2) println(item)
    //    val numberArray = new Array[Int](10)
    //    numberArray(1) = 2
    //    for (item <- numberArray) println(item)
    val strArrayVar = new ArrayBuffer[String]()
    strArrayVar += "Hello"
    strArrayVar += ("World", "Programmer")
    strArrayVar ++= Array("Welcome", "To", "XueTuWuYou")
    //    strArrayVar.trimEnd(3)
    strArrayVar.remove(0, 2)
    strArrayVar.insert(0, "Hi")
    //    println(strArrayVar)
    //遍历数组
    //    for (i <- 0 to strArrayVar.length - 1) print(" " + strArrayVar(i))
    //    println();
    //    for (i <- 0 until strArrayVar.length) print(" " + strArrayVar(i))
    //    println();
    //    for (i <- (0 until strArrayVar.length).reverse) print(" " + strArrayVar(i))
    //    println();
    //    for (i <- strArrayVar if i.startsWith("H")) println(" " + i)
    //    println( strArrayVar.max)
    val fruit = List("Apple", "Banana", "Orange")
    println(fruit(2))
    //    fruit(2) ="" List类型的值无法更新
    var listStr: List[Object] = List("This", "Is", "Covariant", "Example")
    //List类型和其它类型集合一样，它具有协变性（Covariant)，即对于类型S和T，如果S是T的子类型，则List[S]也是List[T]的子类型
    val nums = 1 :: 2 :: 3 :: 4 :: Nil
    println(nums)
    //    strArrayVar(3)="Cheng"
    //    val fruits=new List[String]("Apple","Banana","Orange")
    //    fruits(2)="pear"
    var concatList = List.concat(List('a', 'b'), List('c'))
    println(concatList)
  }

}