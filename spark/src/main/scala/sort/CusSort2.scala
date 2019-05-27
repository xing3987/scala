package sort

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 对数据进行排序
  * 先按收入，后按年龄
  * 传递排序规则，以对象的方式封装规则
  */
object CusSort2 {
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
    //val sorted: RDD[(String, Int, Int)] = tuples.sortBy(tp => new Boy(tp._2, tp._3))
    val sorted: RDD[(String, Int, Int)] = tuples.sortBy(tp => Boy(tp._2, tp._3))
    println(sorted.collect.toBuffer)

    sc.stop()
  }
}

//排序的对象要进行网络传输，要实现序列化
/*
class Boy(val age: Int, val pay: Int) extends Ordered[Boy] with Serializable {
  override def compare(that: Boy): Int = {
    if (that.pay != this.pay) return that.pay - this.pay //收入大的在前面
    else return this.age - that.age //年龄小的在前面
  }
}
*/

//使用case class 因为它默认已经实现了序列化，而且不需要new 创建
case class Boy(age: Int,pay: Int) extends Ordered[Boy] {
  override def compare(that: Boy): Int = {
    if (that.pay != this.pay) return that.pay - this.pay //收入大的在前面
    else return this.age - that.age //年龄小的在前面
  }
}