package scala.demo

import scala.collection.mutable

object CollectionDemo7 {
  def main(args: Array[String]): Unit = {
    //set不可重复的集合，无序的
    val set1 = new mutable.HashSet[Int]()
    //向 HashSet 中添加元素
    set1 += 2
    //add 等价于+=
    set1.add(4)
    set1 ++= Set(1, 3, 5)
    println(set1)
    //删除一个元素
    set1 -= 5
    set1.remove(3)
    println(set1)
    Helper.printlnSplit()

    //map映射
    val map1 = new mutable.HashMap[String, Int]()
    map1("spark") = 1
    map1 += (("hive", 2))
    map1.put("hadoop", 3)
    println(map1)
    map1 -= "spark"
    map1.remove("hive")
    println(map1)
    val map2 = Map[String, Int]("a" -> 2) //直接定义map
    println(map2)
    Helper.printlnSplit()

    //元组(tuple)，可以对指定序号的角标操作
    //元组是类型 Tuple1，Tuple2，Tuple3 等等。目前在 Scala 中只能有 22 个上限，如果您需要更多个元素，那么可以使用集合而不是元
    var t = (1, 2, 3, "hello", 'a')
    var t1 = new Tuple3(4, 5, 6)
    println(t._2, t._4, t1._1)
    t.productIterator.foreach(println) //元组的迭代器操作
    // 对偶元组
    val tuple2 = (1, 3)
    // 交换元组的元素位置
    val swap = tuple2.swap
    println(swap)

    //reduce聚合
    var arr = Array(("a", 2), ("b", 3), ("c", 2))
    println(arr.reduce((x, y) => (x._1, x._2 + y._2)))  //reduce默认x._1结果是第一个值

    var arr1 = Array("hello my name is marry", "hello marry I am lily")
    println(arr1.flatMap(_.split(" ")).map(x => (x, 1)).groupBy(x => x._1).mapValues(t => t.foldLeft(0)(_ + _._2)))
  }
}
