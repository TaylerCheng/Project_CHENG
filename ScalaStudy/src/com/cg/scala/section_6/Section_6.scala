package com.cg.scala.section_6

/**
  *
  * @author ： Cheng Guang
  **/
object Section_6 {
  var studentNo:Int=0;

  def addNo() = {
    studentNo += 1
    studentNo
  }

  def main(args: Array[String]): Unit = {
    var p = new Person("cheng");
    p.name
    p.name_=("shao")
    p.getName
    p.age_=(5)
    print(p)
  }

}



