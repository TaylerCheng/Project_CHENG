package com.cg.scala.section_16

import scala.beans.BeanProperty


class Person[T](var name:T)
//单个泛型参数的使用情况
//声明对象可序列化
//@Serializable
class Student[T,S](name:T,var age:S) extends Person(name){
  @BeanProperty var studentNo:String=null
}

object GenericDemo {
  def main(args: Array[String]): Unit = {
    var student = new Student[String,Int]("摇摆少年梦",18)
    student.setStudentNo("5")
  }
}


