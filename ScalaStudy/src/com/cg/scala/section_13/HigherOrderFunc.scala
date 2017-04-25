package com.cg.scala.section_13

/**
  *
  * @author ： Cheng Guang
  */
object HigherOrderFunc {
  def main(args: Array[String]): Unit = {
    //可以省略匿名函数参数类型
    var array = Array("spark", "hive", "hadoop").map(x => x * 2)
    //    println(array.getClass)

    for (item <- array) {
      //      println(item)
    }

    //List类型
    val list = List("Spark" -> 1, "hive" -> 2, "hadoop" -> 2).map(x => x._1)
    //    println(list.getClass)


    //Map类型
    val map = Map("spark" -> 1, "hive" -> 2, "hadoop" -> 3).map(x => x._1)
    for (item <- map) {
      //      println(item)
    }

    // flatMap函数
    var mergeList = List(List(1, 2, 3), List(2, 3, 4)).flatMap(x => x)
    //    println(mergeList)

    // filter函数
    Array(1, 2, 4, 3, 5).filter(x => x > 3)

    // reduce函数
    var reduce = Array(1, 2, 4, 3, 5).reduce((x, y) => x + y)
    // println(reduce)
    var strReduce = List("Spark", "Hive", "Hadoop").reduce(_ + _)
    //    println(strReduce)

    //6 scan函数
    //从左扫描，每步的结果都保存起来，执行完成后生成数组
    var scanLeft = Array(1, 2, 4, 3, 5).scanLeft(0)((x: Int, y: Int) => x + y)
    for (item <- scanLeft) println(item)
  }
}
