package demo

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
    rdd.map(x => (x.split("//")(1).split("\\.")(0) + "@" + x.split("//")(1).split("/")(1), 1)).reduceByKey(_ + _).sortBy(_._2, false)
      .map(x => (x._1.split("@")(0), (x._1.split("@")(1), x._2))).groupByKey()
      .foreach(x => {
        var it = x._2.iterator.take(3)
        while (it.hasNext) {
          val x2 = it.next()
          print(x._1 + ":" + x2._1 + "->" + x2._2+";")
        }
        println
      })

    sc.stop()
  }
}
