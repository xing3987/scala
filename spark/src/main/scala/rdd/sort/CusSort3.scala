package rdd.sort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 对数据进行排序
  * 先按收入，后按年龄
  * 使用implicit
  */
object CusSort3 {
  //定义一个隐式转换的方法，让UserIm实现Ordered接口（可以提取到一个隐式的公共类中,然后导入即可）
  implicit def UserIm2Orderd(a: UserIm) = new Ordered[UserIm] {
    override def compare(that: UserIm): Int = {
      if (that.pay != a.pay) return that.pay - a.pay //收入大的在前面
      else return a.age - that.age //年龄小的在前面
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    val arr = Array("Tom 30 9999", "Jerry 25 5000", "Kitty 18 30000", "Singer 40 5000", "docter 33 20000")
    val par: RDD[String] = sc.parallelize(arr)
    val tuples: RDD[(String, Int, Int)] = par.map(x => {
      val words: Array[String] = x.split(" ")
      val name = words(0)
      val age = words(1).toInt
      val pay = words(2).toInt
      (name, age, pay)
    })
    val sorted: RDD[(String, Int, Int)] = tuples.sortBy(tp => UserIm(tp._2, tp._3))
    println(sorted.collect.toBuffer)

    sc.stop()
  }
}

//排序的对象要进行网络传输，要实现序列化
case class UserIm(val age: Int, val pay: Int)

