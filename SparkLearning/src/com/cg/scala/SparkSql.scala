package com.cg.scala

import com.cg.scala.pojo.Person
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author ： Cheng Guang
  */
object SparkSql {

  def main(args: Array[String]): Unit = {
    useSparkSession(args)
  }

  def useSparkSession(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .getOrCreate()

    //    import sparkSession.implicits._
    //    val people = sparkSession.read.textFile("F:/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF()
    //读取json
    val people = sparkSession.read.json("F:/people.json").toDF()
    people.createTempView("people")

    val teenagers = sparkSession.sql("SELECT name, age FROM people WHERE age >= 13 AND age <= 24")
    teenagers.collect().foreach(println)

    sparkSession.stop()
  }

  def useSqlContext(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSqlTest").setMaster("local")
    val sc = new SparkContext(conf)
    //    val SparkSession.builder
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val people = sc.textFile("F:/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF()
    people.registerTempTable("people")
    val teenagers = sqlContext.sql("SELECT name, age FROM people WHERE age >= 13 AND age <= 24")
    teenagers.queryExecution //执行过程

    val all = sqlContext.sql("SELECT * FROM people")
    all.collect().foreach(println)

    sc.stop()
  }

}
