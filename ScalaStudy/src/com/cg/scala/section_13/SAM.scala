package com.cg.scala.section_13

import java.awt.event.{ActionEvent, ActionListener}
import javax.swing.JButton

/**
  *
  * @author ： Cheng Guang
  */
object SAM {

  def main(args: Array[String]): Unit = {
    var counter = 0;
    val button = new JButton("click")

    //匿名内部类
    button.addActionListener(new ActionListener {
      override def actionPerformed(event: ActionEvent) {
        counter += 1
      }
    })

    button.addActionListener((event:ActionEvent)=>counter+=1)

  }



}
