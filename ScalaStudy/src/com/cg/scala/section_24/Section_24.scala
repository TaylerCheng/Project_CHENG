package com.cg.scala.section_24

import scala.reflect.runtime.universe._

/**
  * 第二十四节 高级类型（三）
  */
object Section_24 {

  def main(args: Array[String]): Unit = {
    var equles = classOf[List[Int]] == classOf[List[String]]
    println(equles)

    equles = typeOf[List[Int]] == typeOf[List[String]]
    println(equles)
  }

}