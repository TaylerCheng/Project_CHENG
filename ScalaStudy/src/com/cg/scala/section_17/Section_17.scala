package com.cg.scala.section_17



object Section_17 {

  /* 定义了一个隐式函数double2Int，将输入的参数
     从Double类型转换到Int类型
     隐式函数的名称对结构没有影响*/
  implicit def double2Int(x: Double) = x.toInt+10

  implicit def double2String(x: String) = x.toInt


  def main(args: Array[String]): Unit = {
    val x: Int = 3.5;
    println(x)
  }

}


import java.io.File
import scala.io.Source
//RichFile类中定义了Read方法
class RichFile(val file:File){
  def read=Source.fromFile(file).getLines().mkString
}

object ImplicitFunction {

  implicit def double2Int(x: Double) = x.toInt

  implicit def file2RichFile(file: File) = new RichFile(file)


  def main(args: Array[String]): Unit = {
    //在使用时引入所有的隐式方法
//    import cn.scala.xtwy.implicitConversion.ImplicitConversion._
    var x: Int = 3.5
    //隐式函数将java.io.File隐式转换为RichFile类
    val file1 = new File("file.log").read
    val file2 = new File("file.log")
    println(file1.getClass)
    println(file2.getClass)

    x = f(3.14)
    println(x)
  }

  def f(x:Int)=x

}


//class Student(var name:String){
//  //将Student类的信息格式化打印
//  def formatStudent(outputFormat:OutputFormat)={
//    outputFormat.first+" "+this.name+" "+outputFormat.second
//  }
//}
//
//class OutputFormat(var first:String,val second:String)
//
//object ImplicitParameter {
//  def main(args: Array[String]): Unit = {
//    val outputFormat=new OutputFormat("<<",">>")
//    println(new Student("john").formatStudent(outputFormat))
//  }
//}

//如果给函数定义隐式参数的话，则在使用时可以不带参数，代码如下
class Student(var name:String){
  //利用柯里化函数的定义方式，将函数的参数利用
  //implicit关键字标识
  //这样的话，在使用的时候可以不给出implicit对应的参数
  def formatStudent()(implicit outputFormat:OutputFormat)={
    outputFormat.first+" "+this.name+" "+outputFormat.second
  }
}

class OutputFormat(var first:String,val second:String)

object ImplicitParameter {
  def main(args: Array[String]): Unit = {
    //程序中定义的变量outputFormat被称隐式值
    implicit val outputFormat=new OutputFormat("<<",">>")
//    implicit val outputFormat2=new OutputFormat("<<",">>")
    //在.formatStudent()方法时，编译器会查找类型
    //为OutputFormat的隐式值,本程序中定义的隐式值
    //为outputFormat
    println(new Student("john").formatStudent())
  }
}