package com.cg.scala.section_12

import java.io.FileWriter
import java.net.URL

import scala.io.Source

object Section_12 {
  def main(args: Array[String]): Unit = {
    //读取文件
//    val file=Source.fromFile("E:\\gitspace\\Project_CHENG\\ScalaStudy\\src\\com\\cg\\scala\\section_11\\TraitDemo.scala")
    val file= Source.fromURL(new URL("http://www.baidu.com"))

    //返回Iterator[String]
    val lines=file.getLines()
    //打印内容
    for(i<- lines) println(i)
    //关闭文件
    file.close()

  }
}


