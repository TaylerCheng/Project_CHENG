package com.cg.scala.section_22

class Outter{
  private var x:Int=0
  def print(i:Inner)=i
  //内部类Inner
  class Inner{
    def test()=x
  }
}

object TypeProject extends App{
  val outter=new Outter
  //创建内部类的方式，同访问正常的成员变量一样
  val inner=new outter.Inner
//  println(inner.test())

  val outter2=new Outter
  val inner2=new outter2.Inner

  //下面的代码编译会失败
  outter.print(inner)
  //这是因为不同outter对象对应的内部类成员类型是不一样的
  //这就跟两个类成员的实例它们内存地址不一样类似


  //下面的类型判断会输出false
  //这也进一步说明了它们类型是不一样的
  println(inner.getClass == inner2.getClass)
}