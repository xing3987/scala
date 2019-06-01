package rdd.demo

object TypeDemo13 {
  type S = String //定义一个类型S 是String类型
  val name: S = "tom"

  def sayhi(name: S) = {
    println(s"hi $name")
  }

  def main(args: Array[String]): Unit = {
    println(name)
    sayhi("hello")

    val ss = new Student10("Lucy") with TypeTrait14 {
      override type T = Int //重写参数的类型
    }
    ss.fly(123: Int) //调用使用参数的方法
  }
}


