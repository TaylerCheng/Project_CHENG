package com.cg.sparktest.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author ： Cheng Guang
  */
object SimpleWordCount {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: <file>")
      System.exit(1)
    }
    //    wordcount(args)
    handle(args)

  }


  def handle(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ArrayTest").setMaster("local")
    val sc = new SparkContext(conf)

    //    val arrayRdd = sparkContext.parallelize(1 to 9,1);
    //    val smapledA = arrayRdd.sample(false, 0.5,scala.util.Random.nextInt(10000))

    //    val partitions = arrayRdd.mapPartitions(mapPartitionsfunc)
    val rdd1 = sc.parallelize(1 to 5)
    val rdd2 = sc.parallelize(4 to 8)

    //    var resultRdd = rdd1.union(rdd2)//合并
    //    var resultRdd = rdd1.intersection (rdd2)//交集
    //    var resultRdd = rdd1.union(rdd2).distinct()//去重
    //    var resultRdd = rdd1.union(rdd2).map((_,1)).groupByKey()//groupByKey
    //    var resultRdd = rdd1.union(rdd2).map((_,1)).sortByKey()//sortByKey
    //    var resultRdd = rdd1.union(rdd2).map((_,1)).reduceByKey(_+_)//reduceByKey
    //    var resultRdd = rdd1.map((_,1)).join(rdd2.map((_,2)))//join
    var resultRdd = rdd1.cartesian(rdd2) //笛卡尔积
    //        println(rdd1.partitions.size)
    //        var resultRdd = rdd1.coalesce(1)
    //        println(resultRdd.partitions.size)
    resultRdd.countByKey().foreach(println)

    //    rdd1.takeSample(false,3).foreach(println)
    //    rdd2.takeOrdered(3).foreach(println)
    //    println(rdd2.first())
  }

  var i = 0
  def mapPartitionsfunc[T](iter: Iterator[T]): Iterator[(T, T)] = {
    println("分区开始" + i)
    i = i + 1;
    var res = List[(T, T)]()
    var pre = iter.next
    while (iter.hasNext) {
      val cur = iter.next;
      res.::=(pre, cur)
      pre = cur;
    }
    res.iterator
  }

  def indexFunc[T](index: T, iter: Iterator[T]): Iterator[(T, T, T)] = {
    var res = List[(T, T, T)]()
    var pre = iter.next
    while (iter.hasNext) {
      val cur = iter.next
      res.::=(index, pre, cur)
      pre = cur
    }
    res.iterator
  }

  def wordcount(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val line = sc.textFile(args(0), 1)

    var partitions = line.partitions
    for (partition <- partitions) {
      println(partitions)
    }
    //    var add = sc.parallelize(Array(1 to 10), 2)
    //    println(">>>>>>>>>>>>>>>>>>>>>>>> " + add.partitions.size)
    val reduce = line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    println(">>>>>>>>>>>>>>>>>>>>>>>> 开始执行")
    //    reduce.collect().foreach(println)
    reduce.saveAsTextFile("F:/output")
    sc.stop()
  }

}
