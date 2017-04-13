package com.cg.scala.section_9

/**
  *
  * @author ： Cheng Guang
  **/

//抽象的Person类
abstract class People(var name: String, var age: Int) {
  def walk(): Unit

  //talkTo方法，参数为Person类型
  def talkTo(p: People): Unit
}

class CollegeStudent(name: String, age: Int) extends People(name, age) {
  private var studentNo: Int = 0

  def walk() = println("walk like a elegant swan")

  //重写父类的talkTo方法
  def talkTo(p: People) = {
    println("talkTo() method in Student")
    println(this.name + " is talking to " + p.name)
  }
}

class Teacher(name: String, age: Int) extends People(name, age) {
  private var teacherNo: Int = 0

  def walk() = println("walk like a elegant swan")

  //重写父类的talkTo方法
  def talkTo(p: People) = {
    println("talkTo() method in Teacher")
    println(this.name + " is talking to " + p.name)
  }
}

object Section_9 {

  def main(args: Array[String]): Unit = {
    //下面的代码定义了一个匿名类，并且进行了实例化
    //直接new Person("john",18)，后面跟的是类的内容
    //我们知道，Person是一个抽象类，它是不能被实例化的
    //这里能够直接new操作是因为我们扩展了Person类，只不
    //过这个类是匿名的，只能使用一次而已
    //    val s = new People("john", 18) {
    //      override def walk() = {
    //        println("Walk like a normal Person")
    //      }
    //    }
    //    s.walk()

    //下面的两行代码演示了多态的使用
    //Person类的引用可以指向Person类的任何子类
    val p1: People = new Teacher("albert", 38)
    val p2: People = new CollegeStudent("john", 38)

    //下面的两行代码演示了动态绑定
    //talkTo方法参数类型为Person类型
    //p1.talkTo(p2)传入的实际类型是Student
    //p2.talkTo(p1)传入的实际类型是Teacher
    //程序会根据实际类型调用对应的不同子类中的talkTo()方法
    p1.talkTo(p2)
    p2.talkTo(p1)

  }

}




