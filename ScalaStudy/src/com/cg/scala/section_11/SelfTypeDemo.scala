package com.cg.scala.section_11

class A {
  //下面 self =>  定义了this的别名，它是self type的一种特殊形式
  //这里的self并不是关键字，可以是任何名称
  self =>
  val x = 2

  //可以用self.x作为this.x使用
  def foo = self.x + this.x
}

class OuterClass {
  outer => //定义了一个外部类别名
  val v1 = "here"

  class InnerClass {
    // 用outer表示外部类，相当于OuterClass.this
    println(outer.v1)
  }

}

trait X {
  def foo()
}

class B {
  self: X =>
}

//类C扩展B的时候必须混入trait X
//否则的话会报错
class C extends B with X {
  def foo() = println("self type demo")
}

object SelfTypeDemo extends App {
  println(new C().foo)
}