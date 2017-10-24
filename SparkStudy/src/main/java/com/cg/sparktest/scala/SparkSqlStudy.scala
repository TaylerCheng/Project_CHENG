package com.cg.sparktest.scala

import com.cg.sparktest.scala.pojo.Person
import com.cg.sparktest.scala.udf.AddTitleUDF
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author ： Cheng Guang
  */
object SparkSqlStudy {

  def main(args: Array[String]): Unit = {
    useSqlContext(args)
    //    useSparkSession(args)
  }

  def useSqlContext(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSqlTest").setMaster("local")
    val sc = new SparkContext(conf)
    //    val SparkSession.builder
    val sqlContext = new SQLContext(sc)
    val people = sqlContext.read.text("F:/people.txt",",")
    people.registerTempTable("people")

    /**
      * SparkSql UDF两种注册方式
      */
    sqlContext.udf.register("computeLength", (input: String) => input.length)
    sqlContext.udf.register("addTitle", new AddTitleUDF().call _)

    val teenagers = sqlContext.sql("SELECT addTitle(name) as nameLength, age FROM people WHERE computeLength(name) > 3")
    //    teenagers.queryExecution //执行过程
    teenagers.collect().foreach(println)

    //    val all = sqlContext.sql("SELECT * FROM people")
    //    all.collect().foreach(println)

    sc.stop()
  }

  def useSparkSession(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.master("local").appName("spark session example").getOrCreate()

    //    import sparkSession.implicits._
    //    val people = sparkSession.read.textFile("F:/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF()
    //读取json
    val people = sparkSession.read.json("F:/people.json").toDF()
    people.createTempView("people")

    val teenagers = sparkSession.sql("SELECT name,age FROM people WHERE age >= 13 AND age <= 24")
    teenagers.collect().foreach(println)

    sparkSession.stop()
  }

}
