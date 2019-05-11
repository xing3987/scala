package demo

import demo.ClothesEnum.ClothesEnum

/**
  * 泛型，就是类型约束
  */
abstract class Message[T](content: T)

//具体使用
class StrMessage(content: String) extends Message(content)

class IntMessage(content: Int) extends Message(content)

//定义一个泛型衣服类
class Clothes[A, B, C](val clothType: A, val color: B, val size: C){
  override def toString = s"Clothes($clothType, $color, $size)"
}

//定义一个枚举类(object)
object ClothesEnum extends Enumeration {
  type ClothesEnum = Value //值为ClothesEnum类型

  val 上衣, 内衣, 裤子 = Value
}

object Genericity {
  def main(args: Array[String]): Unit = {
    val clothes = new Clothes[ClothesEnum, String, Int](ClothesEnum.上衣, "black", 150)
    println(clothes)
  }
}