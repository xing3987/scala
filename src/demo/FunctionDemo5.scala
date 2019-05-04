package demo

import java.util.Date


object FunctionDemo5 {
  def main(args: Array[String]): Unit = {
    println(addall(1, 2, 3, 4, 5, 6))
    println(addinit(10, 1, 1, 1, 2, 2))
    pringDefault()
    pringDefault("hi")

    println(addtwo())
    println(addtwo(10)) //默认覆盖第一个参数
    println(addtwo(y = 8)) //指定覆盖参数
    Helper.printlnSplit()

    //高阶函数：将其他函数作为参数，或者结果是函数的函数
    println(tostring(layout, 1))


    //部分参数应用,定义另一个函数,函数中给目标函数赋值
    def logbound = log(new Date(), _: String)

    logbound("hello...")
    Helper.printlnSplit()

    //柯里化(Currying)
    println(curry(1)(2))
    val f2 = curry1(1)
    println(f2(3))
    println(curry1(1)(2))
    //偏函数
    val arr1 = Array[Any](1, 2, 3, "a", "b")
    var arr2 = arr1.collect(partial2)
    println(arr2.toBuffer)
  }

  def addall(x: Int*) = { //定义可变参数函数,可变参数放在末尾
    var sum = 0
    for (i <- x) {
      sum += i
    }
    sum
  }

  def addinit(init: Int, x: Int*) = { //定义带初始值的可变参数函数,可变参数放在末尾
    var sum = init;
    for (i <- x) {
      sum += i
    }
    sum
  }

  def pringDefault(x: String = "hello") = println(x) //定义含默认值的函数，如果不传参数则使用默认值

  def addtwo(x: Int = 1, y: Int = 2) = x + y

  def tostring(f: Int => String, v: Int) = f(v) //定义一个高阶函数

  def layout(x: Int) = x.toString

  def log(date: Date, message: String) = {
    println(s"$date : $message")
  }

  def curry(x: Int)(y: Int) = x + y //柯里化多个参数用小括号分开

  def curry1(x: Int) = (y: Int) => x + y //柯里化另一种写法,返回的结果是个函数

  /**
    * 定义偏函数(只能有一个参数)，Partialfunction中第一个参数是传入参数类型，第二个是返回类型
    * 实体要用case复制
    */
  def partial1: PartialFunction[String, Int] = {
    case "a" => 97
    case _ => 0
  }

  //定义偏函数，按照类型匹配
  def partial2: PartialFunction[Any, Int] = {
    case i: Int => i * 10
    case i: String => 0
    case _ => 100
  }
}
