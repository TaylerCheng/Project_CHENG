package com.cg.scala.section_12

/**
  *
  *
  * @author ： Cheng Guang
  */
object RegexMatch {

  def main(args: Array[String]): Unit = {
    //1 匹配邮箱
    var sparkRegex = "^[\\w-]+(\\.[\\w-]+)*@[\\w-]+(\\.[\\w-]+)+$".r
    for (matchString <- sparkRegex.findAllIn("zhouzhihubeyond@sina.com")) {
      println(matchString)
    }

    //2 匹配网址
    sparkRegex = "^[a-zA-Z]+://(\\w+(-\\w+)*)(\\.(\\w+(-\\w+)*))*(\\?\\s*)?$".r
    //  3 匹配手机号码
    sparkRegex = "(86)*0*13\\d{9}".r
    //  4 匹配IP地址
    sparkRegex = "(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)".r
  }

}
