package rdd.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Sort01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Sort01").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //多个文件用 filename\\*结尾
    val lines: RDD[String] = sc.textFile("S:\\logs\\*")
    //两种方法
    //key要转成Int，不然会按String的hascode排序
    lines.sortBy(x => Integer.parseInt(x),false).collect.foreach(println)
    //key要转成Int，不然会按String的hascode排序
    //lines.map(x => (Integer.parseInt(x), x)).sortByKey(false).collect.foreach(x=>println(x._1))
    sc.stop()
  }
}
