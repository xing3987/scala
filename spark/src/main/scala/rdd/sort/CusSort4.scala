package rdd.sort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 对数据进行排序
  * 先按收入，后按年龄
  * 使用元组的特性，作为比较器
  */
object CusSort4 {
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

    //使用默认的元组类作为比较规则，它是先比第一位，然后比第二位，依次...，默认是从小到大，使用“-”号变成从大到小

    /*
    使用隐式比较规则
    implicit val rules = Ordering[(Int, Int)].on[(String, Int, Int)](t => (-t._3, t._2))
    val sorted: RDD[(String, Int, Int)] = tuples.sortBy(tp => tp)
    */

    //直接使用元组特性
    val sorted: RDD[(String, Int, Int)] = tuples.sortBy(tp => (-tp._3, tp._2))
    println(sorted.collect.toBuffer)

    sc.stop()
  }
}


