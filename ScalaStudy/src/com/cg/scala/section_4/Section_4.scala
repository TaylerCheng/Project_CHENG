package com.cg.scala.section_4

object Section_4 {
  def main(args: Array[String]): Unit = {
//    val numberArray=new Array[Int](5)
//    for(i <- 0 until  numberArray.length){
//      println(numberArray(i))
//    }
//    numberArray(2)=5;
//    for(i <- 0 until numberArray.length){
//      println(numberArray(i))
//    }

//    val srcSet = Set(3.0,5)
//    println(srcSet.hashCode())
//    var numsSet=srcSet
//    println(numsSet.hashCode())
//    numsSet= numsSet+6
//    println(numsSet.hashCode())

    val studentInfo=Map("john" -> 21, "stephen" -> 22,"lucy" -> 20)
    var value = studentInfo.get("joh")
    println(value.getClass)
    var i = value.get;
    println(i.getClass)
  }
}


