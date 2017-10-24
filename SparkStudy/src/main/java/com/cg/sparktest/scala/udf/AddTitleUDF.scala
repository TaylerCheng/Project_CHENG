package com.cg.sparktest.scala.udf

/**
  *
  * @author ： Cheng Guang
  */

import org.apache.spark.sql.api.java.UDF1

class AddTitleUDF extends UDF1[String, String] {

  /**
    * 获取数组类型json字符串中某一字段的值
    */
  @throws[Exception]
  override def call(field: String): String = {
    try {
      return "Man：" + field
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    null
  }

}
