package com.cg.scala.section_5

/**
  * 第五节 函数与闭包
  */
object Section_5 {
  def main(args: Array[String]): Unit = {
    /*
       函数字面量 function literal
       =>左侧的表示输入，右侧表示转换操作
    */
    val increase = (x: Int) => x + 1
    println(increase(1))
    def increaseAnother(x: Int) = x + 1
    //数组的map方法中调用（写法1）
    println(Array(1, 2, 3, 4).map(increase).mkString(","))
    //匿名函数写法（写法2)
    println(Array(1, 2, 3, 4).map((x: Int) => x + 1).mkString(","))
    //花括方式（写法3）
    Array(1, 2, 3, 4).map { (x: Int) => x + 1 }.mkString(",")
    //省略.的方式（写法4)
    Array(1, 2, 3, 4) map { (x: Int) => x + 1 } mkString (",")
    //参数类型推断写法（写法5）
    Array(1, 2, 3, 4) map { (x) => x + 1 } mkString (",")
    //函数只有一个参数的话，可以省略()（写法6）
    Array(1, 2, 3, 4) map { x => x + 1 } mkString (",")
    //如果参数右边只出现一次，则可以进一步简化（写法7）
    Array(1, 2, 3, 4) map {
      _ + 1
    } mkString (",")
    //值函数简化方式
    val fun0 = 1 + (_: Int)
    println(fun0(2))
    // 值函数简化方式（正确方式2）
    val fun2: (Double) => Double = 1 + _

    //函数参数(高阶函数）
    def convertIntToString(f:(Int)=>String)=f(4)
    convertIntToString((x:Int)=>x+" s")
    //高阶函数可以产生新的函数
    def multiplyBy(factor:(Int)=>Double)=(x:Double)=>factor(5)*x
    val x=multiplyBy((i:Int)=>2.0*i)
    println(x(50))

    //(x:Int)=>x+more,这里面的more是一个自由变量（Free Variable）,more是一个没有给定含义的不定变量
    //而x则的类型确定、值在函数调用的时候被赋值，称这种变量为绑定变量（Bound Variable）
    var more="S"
    val fun=(x:Int)=>x+more
    println(fun(10))
  }
}