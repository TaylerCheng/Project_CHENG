package com.cg.scala.section_11

import java.io.{IOException, PrintWriter}


trait Logger {
  println("Logger")

  def log(msg: String): Unit
}

trait FileLogger extends Logger {
  println("FilgeLogger")

  //将方法定义为lazy方式
  lazy val fileOutput=new PrintWriter(fileName:String)
//  val fileOutput = new PrintWriter(fileName)
//  fileOutput.println("#")
  //增加了抽象成员变量
  val fileName:String


  def log(msg: String): Unit = {
    fileOutput.print(msg)
    fileOutput.flush()
    fileOutput.close()
  }
}


class Student extends FileLogger{
  //Student类对FileLogger中的抽象字段进行重写
   val fileName="file.log"
}


trait ExceptionLogger extends Exception with Logger{
  def log(msg:String):Unit={
    println(getMessage())
  }
}

//类UnprintedException扩展自ExceptionLogger
//此时ExceptionLogger父类Exception自动成为UnprintedException的父类
//IOException具有父类Exception ExceptionLogger也具有父类Exception
//scala会使UnprintedException只有一个父类Exception
class UnprintedException extends IOException with ExceptionLogger {
  override def log(msg: String): Unit = {
    println("")
  }
}

object TraitDemo {
  def main(args: Array[String]): Unit = {

    //匿名类
//    val s = new {
//      //提前定义
//      override val fileName = "file.log"
//    } with Student
//    s.log("Student");

    val s2=new Student().log("abc")


  }
}


