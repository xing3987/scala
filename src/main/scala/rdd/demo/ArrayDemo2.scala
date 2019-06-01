package rdd.demo

object ArrayDemo2 {
  def main(args: Array[String]): Unit = {

    /**
      * 数组操作
      */
    var arr = Array(1, 2, 3, 4, 5, 6, 6, 6)
    //直接遍历数组for循环
    for (ele <- arr) {
      println(ele)
    }
    Helper.printlnSplit()
    //使用角标遍历
    /*
        var index = 0 to 7 //Array(0, 1, 2, 3, 4, 5, 6, 7)
        for (ele <- index) {
          println(arr(ele))
        }
        */
    for (ele <- 0 to 7) { //0 until 8 左闭右开
      println(arr(ele))
    }
    Helper.printlnSplit()

    for (ele <- 0 until arr.length) { //0 until 8 左闭右开
      println(arr(ele))
    }

    Helper.printlnSplit()

    /**
      * 获取数组中的偶数
      */
    for (ele <- arr if (ele % 2 == 0)) println(ele)
    Helper.printlnSplit()

    /**
      * 双层for循环,分号不能省略
      */
    for (i <- 1 to 3; j <- 1 to 3; if i != j) println(10 * i + j)
    Helper.printlnSplit()

    //yield迭代器
    var datas = for (i <- 1 to 3; j <- 1 to 3; if i != j) yield 10 * i + j
    println("datas:", datas)

  }
}
