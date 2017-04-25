package com.cg.scala.section_14

object PatternMatching extends App{
  var x=0
  for(i<- 1 to 100){
    i  match {
      case 10 => x=10
      case 50 => println(50)
      case 80 => println(80)
      //增加守卫条件
      case _ if(i%4==0)=> println(i+":能被4整除")
      case _ if(i%3==0)=> println(i+":能被3整除")
      case _ =>
    }
  }
}

//抽象类Person
//Person最前面加了个关键字sealed
sealed abstract class Person

//case class Student
case class Student(name:String,age:Int,studentNo:Int) extends Person
//case class Teacher
case class Teacher(name:String,age:Int,teacherNo:Int) extends Person
//case class Nobody
case class Nobody(name:String) extends Person

//SchoolClass为接受多个Person类型参数的类
case class SchoolClass(classDescription:String,persons:Person*)

object CaseClassDemo{
  def main(args: Array[String]): Unit = {
//    //case class 会自动生成apply方法，从而省去new操作
//    val p:Person=Student("john",18,1024)
//    //match case 匹配语法
//    p  match {
//      case Student(name, age, studentNo) => println("Student: " + name + ":" + age + ":" + studentNo)
//      case Teacher(name, age, teacherNo) => println("Teacher: " + name + ":" + age + ":" + teacherNo)
//      case Nobody(name)=>println(name)
//    }

    //定义case class便会自动生成对应的toString,hashCode,equals,copy等方法
    val s=new Teacher("john",38,1024)
    println(s)

    //copy方法是深度拷贝
    val s1=s.copy()
    println(s1 eq s)
    println(s1.hashCode())
    s1.copy(name="stephen")

    val sc=SchoolClass("学途无忧网Scala培训班",Teacher("摇摆少年梦1",27,2015),Student("摇摆少年梦",27,2015))
    sc match{
      case SchoolClass(classDescription,Teacher(name,age,studetNo),Student(name2,age2,studetNo2))=>println(name)
      case _ => println("Nobody")
    }
  }
}