package iplocation

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark通过ip计算出省份和城市
  * ip规则保存在driver机器上
  */
object Ip2Location {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Ip2Location").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //把Ip规则加载到driver端的内存中
    val tuples: Array[(Long, Long, String)] = MyUtils.readRules(args(0))

    //广播ip规则到executor中
    val broadRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(tuples)

    //创建RDD，读取目标日志文件
    val rdd: RDD[String] = sc.textFile(args(1))
    val provinceAndOne: RDD[(String, Int)] = rdd.map(line => {
      val ip = line.split("[|]")(1) //获得ip数据
      val ipLong: Long = MyUtils.ip2Long(ip) //把ip转换成long格式
      val rulesInExecutor: Array[(Long, Long, String)] = broadRef.value //获得广播的规则变量
      val index: Int = MyUtils.binarySearch(rulesInExecutor, ipLong)
      var province: String = "未查到"
      if (index != -1) province = rulesInExecutor(index)._3 //复制查询到的省市
      (province, 1)
    })

    val reduced: RDD[(String, Int)] = provinceAndOne.reduceByKey(_ + _) //统计ip的省市分布信息
    val collected: Array[(String, Int)] = reduced.collect //手机结果到driver端
    println(collected.toBuffer)

    sc.stop()
  }
}
