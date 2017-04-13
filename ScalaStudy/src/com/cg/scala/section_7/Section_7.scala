package com.cg.scala.section_7

/**
  * 第七节：类和对象（二）
  *
  * @author ： Cheng Guang
  */

class Student(var name: String, var age: Int) {
  private var sex: Int = 0

  //直接访问伴生对象的私有成员
  def printCompanionObject() = println(Student.uniqueStudentNo())
}

object Student {
  private var studentNo: Int = 0;

  def uniqueStudentNo() = {
    studentNo += 1
    studentNo
  }

  //定义自己的apply方法
  def apply(name: String, age: Int) = new Student(name, age)

  def main(args: Array[String]): Unit = {
    //    println(Student.uniqueStudentNo())
    //    Section_6.addNo()
    //    print(Section_6.studentNo)
    //    println(new Student("cheng", 26).sex)
    var stu = Student("cheng", 26)
    stu.sex
  }
}

object AppDemo extends App {
  new Dog(10).eat()
}

//scala中的抽象类定义
abstract class Animal {
  //抽象字段(域）
  //前面我们提到，一般类中定义字段的话必须初始化，而抽象类中则没有这要求
  var height: Int

  //抽象方法
  def eat: Unit
}

//Person继承Animal，对eat方法进行了实现
//通过主构造器对height参数进行了初始化
class Dog(var height: Int) extends Animal {
  //对父类中的方法进行实现，注意这里面可以不加override关键字
  def eat() = {
    println("eat by mouth")
  }
}
