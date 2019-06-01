package rdd.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Topn01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Sort01").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //全局topn求数额最大的商品总价
    val lines: RDD[String] = sc.textFile("S:\\logs\\topn.txt")
    val words: RDD[Array[String]] = lines.map(_.split(","))
    val sorted: RDD[Array[String]] = words.sortBy(x => x(3).toDouble * x(4).toDouble, false)
    sorted.collect.foreach(x => {
      for (m <- x) {
        print(m + " ")
      }
      println
    })

    //按订单topn求数额最大的商品总价
    val words2 = lines.map(x => (x.split(",")(0), (x.split(",")(1) + " " +
      x.split(",")(2) + " " + x.split(",")(3) + " " + x.split(",")(4))))
    words2.sortBy(x => x._2.split(" ")(2).toDouble * x._2.split(" ")(3).toDouble, false).groupByKey().foreach(println)
    sc.stop()
  }
}
