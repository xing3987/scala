package rdd.iplocation

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark通过ip计算出省份和城市
  * ip规则保存在hdfs集群中，通过worker读取，然后汇总到driver
  * 取得的结果保存到mysql中去
  */
object Ip2Location2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Ip2Location2").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text: RDD[String] = sc.textFile(args(0))
    //每个worker解析数据
    val tuples: RDD[(Long, Long, String)] = text.map(line => {
      val fields: Array[String] = line.split("\\|")
      val start: Long = fields(2).toLong
      val end: Long = fields(3).toLong
      val province: String = fields(6) + ":" + fields(7)
      (start, end, province)
    })
    //收集到driver端
    val collected: Array[(Long, Long, String)] = tuples.collect
    //如果广播的变量需要实时的改变，可以放到redis集群中
    val broadRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(collected)

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
    /*
        val resulted: Array[(String, Int)] = reduced.collect //搜集结果到driver端
        println(resulted.toBuffer)
    */

    /*
        //将结果保存到mysql中
        reduced.foreach(tp => {
          val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8", "root", "root")
          val pstm: PreparedStatement = conn.prepareStatement("insert into ip_province_count(province,count) values(?,?)")
          pstm.setString(1, tp._1)
          pstm.setInt(2, tp._2)
          pstm.executeUpdate()
          pstm.close()
          conn.close()
        })
    */

    //当数据量大时，需要建立多个conn，占领大量内存。优化：以分区为单位建立连接,因为每个分区会在一个worker上，所以只要建立一个连接
    //把建立连接的方法放入object！！(不要放到class)中，会在executor中初始化，并且是单例的会放入内存中，共同使用,不走网络了不需要序列化
    reduced.foreachPartition(MyUtils.data2Mysql _)

    sc.stop()
  }
}
