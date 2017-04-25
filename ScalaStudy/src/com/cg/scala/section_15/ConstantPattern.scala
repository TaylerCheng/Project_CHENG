package com.cg.scala.section_15

object ConstantPattern {
  def main(args: Array[String]): Unit = {
    //注意，下面定义的是一个函数
    //函数的返回值利用的是模式匹配后的结果作为其返回值
    //还需要注意的是函数定义在main方法中
    //也即scala语言可以在一个函数中定义另外一个函数
    def patternShow(x: Any) = x match {
      case 5 => "five"
      case true => "true"
      case "test" => "String"
      case null => "null"
      case Nil => "empty list"
      case _ => "Other constant"
    }

    println(patternShow(5))

    def patternShow2(x: Any) = x match {
      case 5 => 5
      //所有不是值为5的都会匹配变量y
      //例如"xxx"，则函数的返回结果就是"xxx"
      case y => y
    }

    println(patternShow2("XXX").getClass)
    println(patternShow2(5).getClass)

  }
}

//构造器模式必须将类定义为case class
case class Person(name: String, age: Int)

object ConstructorPattern {

  def main(args: Array[String]): Unit = {
    val p = new Person("摇摆少年梦", 27)
    println(constructorPattern(p))
  }

  def constructorPattern(p: Person) = {
    p match {
      case Person(_,_) => "Person"
      case _ => "Other"
    }
  }

}

// 序列（Sequence)模式
object SequencePattern {
  def main(args: Array[String]): Unit = {
    val p=List("spark","hive","SparkSQL")
    def sequencePattern(p:List[String])=p match {
      //只需要匹配第二个元素
      case List(first,second,_*) => first
      case _ => "Other"
    }
    println(sequencePattern(p))
  }
}

//匹配某个元组内容
object TuplePattern {
  def main(args: Array[String]): Unit = {
    val t=("spark","hive","SparkSQL")
    def tuplePattern(t:Any)=t match {
      case (one,_,_) => one
      case _ => "Other"
    }
    println(tuplePattern(t))
  }
}

//匹配传入参数的类型
object TypePattern {
  def main(args: Array[String]): Unit = {
    println(tuplePattern(5.0))
    println(tuplePattern2("a"))
  }

  def tuplePattern(t: Any) = t match {
    case t: String => "String"
    case t: Int => "Integer"
    case t: Double => "Double"
  }

  //  上述代码如果不用模式匹配的话，要实现相同的功能，可以通过下列代码实现：
  def tuplePattern2(t: Any) = {
    if (t.isInstanceOf[String]) "String"
    else if (t.isInstanceOf[Int]) "Int"
    else if (t.isInstanceOf[Double]) "Double"
    else if (t.isInstanceOf[Map[_, _]]) "MAP"
  }
}

//变量绑定模式
object VariableBindingPattern {
  def main(args: Array[String]): Unit = {
    var t=List(List(1,2,3),List(2,3,4))
    def variableBindingPattern(t:Any)= t match {
      //变量绑定，采用变量名（这里是e)
      //与@符号，如果后面的模式匹配成功，则将
      //整体匹配结果作为返回
      case List(_,e@List(_,_,_)) => e
      case _ => Nil
    }

    println(variableBindingPattern(t))
  }
}

//2. for控制结构中的模式匹配
object PatternInForLoop {
  def main(args: Array[String]): Unit = {
    val m = Map("china" -> "beijing", "dwarf japan" -> "tokyo", "Aerican" -> "DC Washington")
    //利用for循环对Map进行模式匹配输出，
    for ((nation, capital) <- m)
      println(nation + ": " + capital)
  }
}
object RegexMatch {
  def main(args: Array[String]): Unit = {
    val ipRegex="(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)".r
    for(ipRegex(one,two,three,four) <- ipRegex.findAllIn("192.168.1.1"))
    {
      println("IP子段1:"+one)
      println("IP子段2:"+two)
      println("IP子段3:"+three)
      println("IP子段4:"+four)
    }
  }
}

object OptionDemo extends App{
  val m=Map("hive"->2,"spark"->3,"Spark MLlib"->4)

  def mapPattern(t:String)=m.get(t) match {
    case Some(x) => println(x);x
    case None => println("None");-1
  }

  println(mapPattern("hive"))
}