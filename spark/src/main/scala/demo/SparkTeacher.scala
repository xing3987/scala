package demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 求最受欢迎的老师，多个task要先collect到driver上再输出
  * 1.在所有的老师中求出最受欢饮的老师Top3
  * 2.求每个学科中最受欢迎老师的top3
  */
object SparkTeacher {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkTeacher").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("G:\\logs\\teacher.log")
    //1.在所有的老师中求出最受欢饮的老师Top3, 先collect到driver上再输出,take(n)去前几个结果(.reverse.take(n)去最后几个)
    rdd.map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false).collect.take(3).foreach(x => {
      println("老师：" + x._1.split(".cn/")(1) + ",欢迎度：" + x._2)
    })

    //2.求每个学科中最受欢迎老师的top3 用.分割要转义"\\"或者"[.]"
    val maps: RDD[((String, String), Int)] = rdd.map(x => ((x.split("//")(1).split("\\.")(0), x.split("//")(1).split("/")(1)), 1))
    //分组聚合
    val groups: RDD[(String, Iterable[((String, String), Int)])] = maps.reduceByKey(_ + _).groupBy(_._1._1)
    //组内求topn，mapValues对值进行操作，x => x.toList.sortBy(_._2).reverse.take(3)为scala的方法
    val topn: RDD[(String, List[((String, String), Int)])] = groups.mapValues(x => x.toList.sortBy(_._2).reverse.take(3))
    //打印输出结果
    //topn.foreach(println)
    topn.foreach(x => {
        var it = x._2.iterator
        while (it.hasNext) {
          val x2 = it.next()
          print(x._1 + ":" + x2._1._2 + "->" + x2._2+";")
        }
        println
      })

    sc.stop()
  }
}
