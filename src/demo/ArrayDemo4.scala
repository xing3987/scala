package demo

object ArrayDemo4 {
  def main(args: Array[String]): Unit = {
    var arr = Array(1, 2, 3, 4)
    arr = arr.map((a: Int) => a * 3) //定义数据计数方法，传递一个匿名计数函数
    println(arr.toBuffer) //toBuffer打印内容

    arr = arr.map(f1) //定义数据计数方法，传递一个计数方法
    println(arr.toBuffer) //toBuffer打印内容
  }

  def f1(a: Int): Int = a * 2
}
