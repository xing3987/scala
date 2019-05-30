package sort

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkSerial {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSerial").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val list = List("2019年1月6日，星期五，12:40:25 Tom 18005478512 12",
      "2019年1月3日，星期三，9:20:35 Marry 18005470002 15",
      "2019年1月4日，星期四，13:50:15 Kitty 15605478512 32",
      "2019年1月5日，星期日，12:11:32 Jam 14305478512 45")

    val lines: RDD[String] = sc.parallelize(list)
    val maped: RDD[(Long, String, String, String)] = lines.map(line => {
      val words: Array[String] = line.split(" ")
      (MyUtils.parse(words(0)), words(1), words(2), words(3))
    })
    val sorted: RDD[(Long, String, String, String)] = maped.sortBy(tp => (tp._1))
    println(sorted.collect.toBuffer)
    sc.stop()
  }
}

object MyUtils {

  //在executor中使用格式化日期，是多线程的所以不能使用SimpleDateFormat,会报格式化错误
  //可以使用读写锁，或者使用FastDateFormat
  def parse(str: String): Long = {
    val sf: FastDateFormat = FastDateFormat.getInstance("yyyy年MM月dd日，E，HH:mm:ss")
    sf.parse(str).getTime
  }
}