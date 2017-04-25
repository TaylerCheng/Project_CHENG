package com.cg.scala.section_17

package cn.scala.xtwy
import java.io.File
import scala.io.Source

package implicitConversion{
  object ImplicitConversion{
    implicit def double2Int(x:Double)=x.toInt
    implicit def file2RichFile(file:File)=new RichFile(file)
    //implicit def file2RichFile2(file:File)=new RichFile(file)
    implicit def richFile2RichFileAnother(file:RichFile)=new RichFileAnother(file)
  }

}

class RichFile(val file:File){
  def read=Source.fromFile(file).getLines().mkString
}

//RichFileAnother类，里面定义了read2方法
class RichFileAnother(val file:RichFile){
  def read2=file.read
}



object ImplicitFunction extends App{
  import cn.scala.xtwy.implicitConversion.ImplicitConversion._
  var x:Int=3.5

  //隐式转换不会多次进行，下面的语句会报错
  //不能期望会发生File到RichFile，然后RifchFile到
  //RichFileAnthoer的转换
//  val f=new File("file.log").read2
//  println(f)
}


