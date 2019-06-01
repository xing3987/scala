package rdd.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object STjoin {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Sort01").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val line1: RDD[String] = sc.textFile("S:\\logs\\stjoin.txt")
    val value1: RDD[((String, String))] = line1.map(x => ((x.split(" ")(0), x.split(" ")(1) + " " + "S")))

    val value2: RDD[((String, String))] = line1.map(x => ((x.split(" ")(1), x.split(" ")(0) + " " + "P")))

    value1.join(value2).foreach(x => {
      println(x._2._2.split(" ")(0) + " grandparents: " + x._2._1.split(" ")(0))
    })
    sc.stop()
  }
}
