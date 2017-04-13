package com.cg.scala.section_10

object Section_10 {
  def main(args: Array[String]): Unit = {
    var v = ()
    println(v.getClass)

    val x=new String("123")
    val y=new String("123")
    println(x==y)
    println(x.equals(y))
    println(x.eq(y))
    println(x.ne(y))



    //Null类型是所有AnyRef类型的子类型，也即它处于AnyRef类的底层，对应java中的null引用。而Nothing是scala类中所有类的子类，它处于scala类的最底层。
//    var z:Int=Nothing
    var str:String = null
//    println(str.getClass)
  }

}


trait DAO{
  //定义一个抽象方法，注意不需要加abstract
  //加了abstract反而会报错
  def delete(id:String):Boolean
  def add(o:Any):Boolean
  def update(o:Any):Int
  def query(id:String):List[Any]
}

trait MysqlDAO {
  var recodeMount:Long=15000000000000L

  def add(o:Any):Boolean
  def update(o:Any):Int
  def query(id:String):List[Any]
  //delete方法有具体实现
  def delete(id:String):Boolean={
    println("delete implementation")
    true
  }
}

class DaoImpl extends MysqlDAO with Cloneable{
  def add(o:Any):Boolean=true
  def update(o:Any):Int= 1
  def query(id:String):List[Any]=List(1,2,3)
}

