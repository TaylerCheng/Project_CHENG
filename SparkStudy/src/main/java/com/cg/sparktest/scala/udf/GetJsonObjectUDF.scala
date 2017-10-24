package com.cg.sparktest.scala.udf

/**
  *
  *
  * @author ： Cheng Guang
  */

import org.apache.spark.sql.api.java.UDF2
import com.alibaba.fastjson.JSON

/**
  * 用户自定义函数
  *
  * @author pengyucheng
  */
class GetJsonObjectUDF extends UDF2[String, String, String] {

  /**
    * 获取数组类型json字符串中某一字段的值
    */
  @throws[Exception]
  override def call(json: String, field: String): String = {
    try {
      val jsonObject = JSON.parseObject(json)
      return jsonObject.getJSONArray(field).getString(0)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    null
  }

}
