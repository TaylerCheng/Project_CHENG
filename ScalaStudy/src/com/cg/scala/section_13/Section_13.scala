package com.cg.scala.section_13

object Section_13 {
  def main(args: Array[String]): Unit = {
    // 普通函数写法
    println(simpleMultiply(4, 5))
    //普通函数柯里化
    println(simpleCurryMultiply(4)(5))
    //返回一个函数
    var c1 = simpleMultiply(4, _: Double)
    var c2 = simpleCurryMultiply(4) _
    println(c1.getClass)
    println(c2.getClass)
    println(c1(6))
    println(c2(6))


    //1、函数参数，即传入另一个函数的参数是函数
    var str = convertIntToString((x: Int) => x + " s")
    //    println(str)

    // 2、返回值是函数的函数
    var func = multiplyBy(10.0)
    var result = func(50)
    //    println(result)
    //这是高阶函数调用的另外一种形式
    //    println(multiplyBy(10)(50))

    //3. 函数柯里化
    //    println(multiplyByCurry(10)(50))
    var curryFunc = curryMultiplyBy(10) _
    curryFunc(50)

    //  部分应用函数
    def myprint(x: String) = print(x + " ")

    //    Array("Hadoop","Hive","Spark").foreach(myprint)
    //    println()
    //定义print的部分应用函数
    val p = myprint _
    //    Array("Hadoop","Hive","Spark")foreach(p)

    //定义一个求和函数
    def sum(x: Int, y: Int, z: Int) = x + y + z

    val s1 = sum _
    //    println(s1(1,2,3))
    //指定两个参数的部分应用函数
    val s2 = sum(1, _: Int, 3)
    println(s2(2))
    //指定一个参数的部分应用函数
    val s3 = sum(1, _: Int, _: Int)
    println(s3(2, 3))
  }

  def simpleMultiply(x: Double, y: Double): Double = return x * y

  def simpleCurryMultiply(x: Double)(y: Double): Double = return x * y

  def convertIntToString(f: (Int) => String) = f(4)

  def multiplyBy(factor: Double) = (x: Double) => factor * x

  def curryMultiplyBy(factor: Double)(x: Double) = x * factor

}


