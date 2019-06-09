package sparksteam

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * sparkstreaming 整合kafka
  */
object KafkaWordCount2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

    val zkaddress = "hadoop001:2181,hadoop002:2181,hadoop003:2181"
    val groupId = "myId"
    //主题和对应使用的线程
    val topic = Map[String, Int]("wordcount" -> 1)

    //连接kafka,创建KafkaDStream
    val datas: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkaddress, groupId, topic)
    //获得DStream(Kafak的ReceiverInputDStream[(String, String)]里面装的是一个元组（key是写入的key，value是实际写入的内容)）
    val lines: DStream[String] = datas.map(_._2)

    val words: DStream[String] = lines.flatMap(_.split(" "))
    val reduced: DStream[(String, Int)] = words.map((_,1)).reduceByKey(_+_)

    reduced.print()
    //启动sparksteaming程序
    ssc.start()
    //等待优雅的退出
    ssc.awaitTermination()
  }
}
