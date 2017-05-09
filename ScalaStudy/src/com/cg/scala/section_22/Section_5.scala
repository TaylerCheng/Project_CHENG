package com.cg.scala.section_22

//1. this.type使用
class Person {
  private var name: String = null
  private var age: Int = 0

  def setName(name: String): this.type = {
    this.name = name
    //返回对象本身
    this
  }

  def setAge(age: Int): this.type = {
    this.age = age
    //返回对象本身
    this
  }

  override def toString() = "name:" + name + " age:" + age
}

class Student extends Person {
  private var studentNo: String = null

  def setStudentNo(no: String) = {
    this.studentNo = no
    this
  }

  override def toString() = super.toString() + " studetNo:" + studentNo
}

object Main extends App {
  //链式调用
  println(new Person().setAge(18).setName("摇摆少年梦"))
  //下面的代码会报错
  //value setStudentNo is not a member of cn.scala.xtwy.advancedtype.Person
  println(new Student().setName("john").setAge(22).setStudentNo("2014"))
  //  Student对象在调用setName、setAge方法时，返回的对象类型实质上仍然是Person类型，而Person类型并没有setStudentNo方法，从而编译出错。
  // 为解决该问题，可以将setName、setAge方法的返回值设置为：this.type
}


