package scala.demo

/**
  * 方法对多可以传递22个参数
  */
object FunctionDemo3 {
  def main(args: Array[String]): Unit = {
    println(add(1, 3))
    sayhello
    sayhello _ //方法加" _"可以转成函数
    println(f1(3, 6))
    println(f2(6, 2))
    Helper.printlnSplit()

    //方法的传值调用和传名调用
    printByValue(count())
    Helper.printlnSplit()
    printByName(count)
    Helper.printlnSplit()
    //高阶函数
    println(add3(add2, 3, 3))
  }

  val f1 = (a: Int, b: Int) => a + b //定义函数(第一种)

  val f2: (Int, Int) => Int = (x, y) => x + 2 * y //定义函数（第二种）

  def add(x: Int, y: Int): Int = x + y //def定义方法，然后是方法名，参数，返回值类型，然后是方法体

  def sayhello = println("hello my name is xxx") //定义无参数的方法，可以不写括号

  (a: Int, b: Int) => a + b //定义匿名函数(不可调用)

  var money = 100

  def pending() = {
    money = money - 5
  }

  def count(): Int = {
    pending()
    money
  }

  def printByValue(x: Int): Unit = { //方法的传值调用,先计算完方法后传值
    for (a <- 0 until 3) {
      println(s"剩余${x}元")
    }
  }

  def printByName(x: => Int) = { //方法的传名调用,把函数传入，在方法内部执行
    for (a <- 0 to 3) {
      println(s"剩余${x}元")
    }
  }

  def add3(f: (Int, Int) => Int, x: Int, y: Int) = { //定义了一个方面，内部含有一个函数
    f(x, y)
  }

  def add2(x: Int, y: Int) = x * 2 + y //定义一个函数，用于传入add3
}
