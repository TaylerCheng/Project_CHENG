package com.cg.scala.section_4

import scala.collection.immutable.{Queue, Stack}


/**
  * 第四节 Set、Map、Tuple、队列操作实战
  * @author ： Cheng Guang
  */
object Section_4 {
  def main(args: Array[String]): Unit = {
    /**
      * 1、Set
      */
    var mutableSet = Set(1, 2, 3)
    println(Set.getClass)
    var tempSet = mutableSet
    println(mutableSet.equals(tempSet))
    mutableSet += 6 //这个时候创建了一个新的对象
    println(mutableSet.equals(tempSet))
    println(mutableSet)

    /**
      * 2、map
      */
    var studentInfo = Map("john" -> 21, "stephen" -> 22, "lucy" -> 20)
    //实际上创建了新的对象
    studentInfo += "taylor" -> 26
    println(studentInfo)
    //遍历操作1
    //    for (i <- studentInfo) println(i.getClass);
    //遍历操作2
    //        studentInfo.foreach(e => {
    //          val (k, v) = e;
    //          println(k + ":" + v)
    //        }
    //遍历操作3
    //    studentInfo.foreach(e =>
    //      println(e._1 + ":" + e._2)
    //    )
    val xMap = scala.collection.mutable.Map(("spark", 1), ("hive", 1))
    xMap.get("spark")
    System.out.println(xMap.getClass);
    println(xMap.get("spark").getClass)

    /**
      * 3、元组
      */
    var tuple = ("Hello", "China", 1)
    println(tuple.getClass)

    /**
      * 4、队列
      */
    var queue = Queue[Int](1, 2, 3)
    var temp = queue
    //出队
    queue.dequeue
    println(temp.equals(queue))
    //入队
    queue.enqueue(4)
    //集合方式
    queue enqueue (List(6, 7, 8))
    var queue2 = scala.collection.mutable.Queue[Int](1, 2, 3, 4, 5)
    var temp2 = queue2
    queue.enqueue(5)
    println(temp2.equals(queue2))

    /**
      * 5、栈
      */
    val stack = Stack[Int](1, 2, 3)
    //出栈
    stack.top
    //入栈
    stack.push(1)
  }
}
