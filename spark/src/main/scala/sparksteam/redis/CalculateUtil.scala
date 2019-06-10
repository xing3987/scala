package sparksteam.redis

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import rdd.iplocation.MyUtils
import redis.clients.jedis.Jedis

/**
  * 用于计算具体的业务数据
  */
object CalculateUtil {

  //计算成交总金额
  def calculateIncome(fields: RDD[Array[String]]) = {
    //获取每个价格
    val price: RDD[Double] = fields.map(arr => arr(4).toDouble)
    //将当前批次的总金额返回了
    val sum: Double = price.reduce(_ + _)
    //获取连接
    val conn: Jedis = JedisConnectionPool.getConnection()
    conn.incrByFloat(Constant.TOTAL_INCOME, sum)
    //关闭连接
    conn.close()
  }

  //计算商品分类金额
  def calculateItem(fields: RDD[Array[String]]) = {
    val prices: RDD[(String, Double)] = fields.map(arr => {
      val item: String = arr(2)
      val price: Double = arr(4).toDouble
      (item, price)
    })
    val sum: RDD[(String, Double)] = prices.reduceByKey(_ + _)
    sum.foreachPartition(part => {
      //获取一个Jedis连接
      //这个连接其实是在Executor中的获取的
      //JedisConnectionPool在一个Executor进程中有几个实例（单例）
      val conn: Jedis = JedisConnectionPool.getConnection()
      part.foreach(t => {
        conn.incrByFloat(t._1, t._2)
      })
      //将当前分区中的数据跟新完在关闭连接
      conn.close()
    })
  }

  //计算区域成交金额
  def calculateZone(fields: RDD[Array[String]], broadcastRef: Broadcast[Array[(Long, Long, String)]]) = {
    val prices: RDD[(String, Double)] = fields.map(arr => {
      val ip: String = arr(1)
      val ipLong: Long = MyUtils.ip2Long(arr(1))
      val rules: Array[(Long, Long, String)] = broadcastRef.value
      val index: Int = MyUtils.binarySearch(rules, ipLong)
      var province = "未知"
      if (index != -1) {
        province = rules(index)._3
      }
      val price: Double = arr(4).toDouble
      (province, price)
    })

    val sum: RDD[(String, Double)] = prices.reduceByKey(_ + _)
    sum.foreachPartition(part => {
      //获取一个Jedis连接
      //这个连接其实是在Executor中的获取的
      //JedisConnectionPool在一个Executor进程中有几个实例（单例）
      val conn: Jedis = JedisConnectionPool.getConnection()
      part.foreach(t => {
        conn.incrByFloat(t._1, t._2)
      })
      //将当前分区中的数据跟新完在关闭连接
      conn.close()
    })
  }

}
