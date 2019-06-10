package sparksteam.redis

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext

/**
  * 广播ip规则
  */
object IPUtils {

  //获取ip规则，并官博到executor
  def broadcastIpRules(ssc: StreamingContext, ipRulesPath: String): Broadcast[Array[(Long, Long, String)]] = {
    val sc = ssc.sparkContext
    val lines: RDD[String] = sc.textFile(ipRulesPath)
    val ipRDD: RDD[(Long, Long, String)] = lines.map(line => {
      val fields: Array[String] = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      (startNum, endNum, province)
    })
    //收集数据到driver
    val tuples: Array[(Long, Long, String)] = ipRDD.collect()
    //广播变量，得到一个引用
    sc.broadcast(tuples)
  }
}
