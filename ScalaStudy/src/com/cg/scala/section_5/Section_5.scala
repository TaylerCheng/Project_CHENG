package com.cg.scala.section_5


object Section_5 {

  def main(args: Array[String]): Unit = {
    var more = 1
    if (Math.random() > 0.5) {
      var more = 2
    } else {
      var more = "2s"
    }
    //函数闭包
    val fun = (x: Int) => x + more
    print(fun(4))
  }

}


