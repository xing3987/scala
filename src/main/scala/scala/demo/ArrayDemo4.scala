package scala.demo

object ArrayDemo4 {
  def main(args: Array[String]): Unit = {
    var arr = Array(1, 2, 3, 4)
    arr = arr.map((a: Int) => a * 3) //定义数据计数方法，传递一个匿名计数函数
    println(arr.toBuffer) //toBuffer打印内容

    def f1(a: Int): Int = a * 2

    arr = arr.map(f1) //定义数据计数方法，传递一个计数方法
    println(arr.toBuffer) //toBuffer打印内容

    val arr1 = new Array[String](3) //定义一个定长的数组，内容可以变
    arr1(0) = "a"
    arr1(1) = "b"
    arr1(2) = "c"
    println(arr1.toBuffer)

    var arr2 = new Array[String](3) //定义一个定长的数组，长度可变，内容可以变
    arr2 = new Array[String](4)
    arr2(0) = "a"
    arr2(1) = "b"
    arr2(2) = "c"
    arr2(3) = "d"
    println(arr2.toBuffer)
    Helper.printlnSplit()

    //数组的操作,arr经过map操作后会返回一个新的数组
    println(arr.map((x: Int) => x + 1).toBuffer)
    //简化
    println(arr.map(x => x + 1).toBuffer)
    println(arr.map(_ + 1).toBuffer)
    Helper.printlnSplit()
    //map|flatten|flatMap|foreach 方法的使用
    var arr3 = Array("hello my name is marry", "hello marry I am lily")
    println(arr3.toBuffer)
    val arr4: Array[Array[String]] = arr3.map(_.split(" "))
    println(arr4.toBuffer)
    val arr5: Array[String] = arr4.flatten //flatten合并
    println(arr5.toBuffer)
    //直接使用flatMap简化操作
    println(arr3.flatMap(_.split(" ")).toBuffer)
    //foreach遍历
    arr3.flatMap(_.split(" ")).foreach(println)
    arr5.foreach(println(_))
    Helper.printlnSplit()

    //worldcount
    val tomap = arr3.flatMap(_.split(" ")).groupBy(x => x) //通过每个元素分组
    println(tomap)
    println(tomap.map(x => x._2.length))
    println(tomap.mapValues(_.length)) //mapValues去所有的值，_.length求值的长度
    println(tomap.mapValues(_.length).toList) //转成数组
    println(arr3.flatMap(_.split(" ")).groupBy(x => x).mapValues(_.length).toList)//转成数组用于排序
    println(arr3.flatMap(_.split(" ")).groupBy(x => x).mapValues(_.length).toList.sortBy(x => -x._2))  //排序
  }
}
