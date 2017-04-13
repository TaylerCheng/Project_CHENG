package com.cg.scala.section_6

import scala.beans.BeanProperty

/**
  *
  *
  * @author ： Cheng Guang
  */
//只有辅助构造函数的类
class Person(@BeanProperty var name:String){
  //类成员
  var age: Int = 18
  @BeanProperty var sex: Int = 0

  //辅助构造器
  def this(name: String, age: Int) {
    this(name)
    this.age = age
  }

  def this(name: String, age: Int, sex: Int) {
    this(name, age)
    this.sex = sex
  }

  override def toString: String = name + ":" + age + ": " + sex
}
