package com.cg.scala.section_18

object ImplicitParameter extends App {
//  //下面的代码不能编译通过
//  //这里面泛型T没有具体指定，它不能直接使用
//  //<符号进行比较
//  def compare[T](first:T,second:T)={
//    if (first < second)
//      first
//    else
//      second
//  }

  //指定T的上界为Ordered[T]，所有混入了特质Ordered
  //的类都可以直接的使用<比较符号进行比较
//  def compare[T<:Ordered[T]](first:T,second:T)={
//    if (first < second)
//      first
//    else
//      second
//  }

  //还有一种解决方案就是通过隐式参数的隐式转换来实现，代码如下：
  //下面代码中的(implicit order:T=>Ordered[T])
  //给函数compare指定了一个隐式参数
  //该隐式参数是一个隐式转换
  def compare[T](first:T,second:T)(implicit order:T=>Ordered[T])={
    if (first > second)
      first
    else
      second
  }
  println(compare("A","B"))


  //implicit关键字在下面的函数中只能出现一次
  //它作用于两个参数x,y，也即x,y都是隐式参数
  def sum(implicit x: Int, y: Int) = x + y
  //下面的函数不合法，函数如果没有柯里化，不能期望
  //implicit关键字会作用于其中一个参数
  //def sum(x: Int, implicit y: Int) = x + y
  //def sum(implicit x: Int, implicit y: Int) = x + y

  //只能指定一个隐式值
  //例如下面下定义的x会自动对应maxFunc中的
  //参数x,y即x=3,y=3，从而得到的结果是6
  implicit val x:Int=3
  //不能定义两个隐式值
  //implicit val y:Int=4
//  println(sum)
//  println(sum(4,5))

  //要点2：要想使用implicit只作用于某个函数参数，则需要将函数进行柯里化
  def sum2(x: Int)(implicit y:Int)=x+y
  println(sum2(5))

//  值得注意的是，下面这种两种带隐式参数的函数也是不合法的
//def sum(x: Int)(implicit y:Int)(d:Int)=x+y+d
//  def sum(x: Int)(implicit y:Int)(implicit d:Int)=x+y+d

  // 要点3: 匿名函数不能使用隐式参数，例如：
//  val sum2=(implicit x:Int)=>x+1


}

object Main extends App {
  class PrintOps() {
    def print(implicit i: Int) = println(i);
  }

  implicit def str2PrintOps(s: String) = {
    println("str2PrintOps")
    new PrintOps
  }

  implicit def str2int(implicit s: String): Int = {
    println("str2int")
    Integer.parseInt(s)
  }

  implicit def getString = {
    println("getString")
    "123"
  }
  //下面的代码会发生三次隐式转换
  //首先编译器发现String类型是没有print方法的
  //尝试隐式转换，利用str2PrintOps方法将String
  //转换成PrintOps（第一次）
  //然后调用print方法，但print方法接受整型的隐式参数
  //此时编译器会搜索隐式值，但程序里面没有给定，此时
  //编译器会尝试调用 str2int方法进行隐式转换，但该方法
  //又接受一个implicit String类型参数，编译器又会尝试
  //查找一个对应的隐式值，此时又没有，因此编译器会尝试调用
  //getString方法对应的字符串（这是第二次隐式转换，
  //获取一个字符串，从无到有的过程）
  //得到该字符串后，再调用str2int方法将String类型字符串
  //转换成Int类型（这是第三次隐式转换）
  "a".print
}
