package demo

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object CollectionDemo6 {
  def main(args: Array[String]): Unit = {
    //定义变长数组
    val arr = ArrayBuffer[Int]()
    arr += 1
    arr += 2
    arr += 3
    arr += (3, 4) //+=往数组中添加元素
    arr ++= ArrayBuffer(5, 6, 7) //++=往数组中添加数组
    println(arr.toBuffer)

    arr.insert(2, 1) //在数组的某个位置插入元素
    println(arr)

    arr.remove(2, 2) //删除数组中某个位置开始的几个元素
    println(arr)
    Helper.printlnSplit()

    //Nil表示空序列,不可变的序列操作会生成新的序列
    var arr1 = Nil
    println(arr1)

    //:: 表示往序列前面插入元素四种方法
    var arr2 = 0 :: arr1
    val arr3 = (1, 2) :: arr2
    println(arr3)
    println(arr2)
    arr2 = arr2.::(1)
    println(arr2)
    arr2 = 2 +: arr2
    println(arr2)
    arr2 = arr2.+:(3)
    println(arr2)

    //将一个元素添加到序列的后面产生一个新的序列
    var arr4 = arr2 :+ 3
    println(arr4)

    //合并两个集合,三种方式
    var arr5 = arr2 ::: arr4
    println(arr5)
    arr5 = arr2 ++ arr4
    println(arr5)
    arr5 = arr2 ++: arr4
    println(arr5)
    Helper.printlnSplit()

    //可变序列，操作不会生成新的序列，会改变原有的序列
    val list1 = ListBuffer(1, 2, 3)
    list1 += 4
    list1.append(5)
    println(list1)
    val list2 = ListBuffer(4, 5, 6)
    list1 ++= list2
    println(list1)
    //也可以使用对不可变序列部分的操作，但会生成新的序列
    var list3 = list1 ++: list2
    println(list3)
    list3 = list1 ++ list2
    println(list3)
    list3 = list1 :+ 6
    println(list3)
    list3 = 0 +: list1
    println(list3)
    Helper.printlnSplit()

    /**
      * 集合的常用方法
      * map, flatten, flatMap, filter, sorted, sortBy, sortWith, grouped, fold(折叠),
      * foldLeft, foldRight, reduce, reduceLeft, aggregate, union,intersect(交集),
      * diff(差集), head, tail, zip, mkString, foreach, length, slice, sum
      */
    println(list3)
    list3 = list3.map(x => x + 1) //对集合内元素进行操作，传递一个操作的方法，返回一个新的集合
    println(list3)
    var list4 = Array("hello world", "hello lily")
    var list5 = list4.map(_.split(" ")).flatten //对集合内元素进行操作，传递一个操作的方法，返回一个新的集合
    println(list5.toBuffer)
    println(list4.flatMap(_.split(" ")).toBuffer)

    println(list3.sorted) //排序，默认升序
    var list6 = Array((1, 3), (3, 2))
    println(list6.sortBy(_._2).toBuffer) //通过第二个元数排序
    println(list6.sortWith((x, y) => -x._1 > y._1).toBuffer) //自定义排序规则，按照第一个元素大小排序
    println(list3.sortWith((x, y) => x > y)) //自定义降序排列

    println(list3.grouped(2).toBuffer) //grouped按照个数分组
    Helper.printlnSplit()

    //fold叠加运算，如果是right就把初始值放到右边，然后从右边开始计数；默认是放左边从左开始运算
    var list7 = Array(1, 3, 5)
    println(list7.fold(1)(_ + _)) //第一个参数是初始值，第二个参数是运算规则 (((1+1)+3)+5)默认是从左边开始加
    println(list7.foldRight(1)(_ - _)) //1-(3-(5-1))从右边开始计算

    println(list3.union(list7)) //并集
    println(list3.intersect(list7)) //交集
    println(list3.diff(list7)) //差集

    var list8 = list3.zip(list7) //除zip把两个集合对应角标的位置组成新的list,多出来的直接去
    println(list8)
    var list9 = list8.map(x => x._1 + x._2)
    println(list9)

    println(list9.mkString("-")) //转成字符串，添加拼接符
    println(list9.sum)
    println(list3.length)
    println(list3)
    println(list3.slice(3, list3.length)) //集合的截取，不含前，含后
    println(list3.slice(5, list3.length).map(_ * 10)) //去数组的第五个开始到结束每个乘以10
  }
}
