package sparksteam

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * sparkstreaming 整合kafka,实现单词结果累加
  */
object KafkaWordCount3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

    //如果要使用更新历史数据（累加），那么就要把中间结果保存起来，使用checkpoint
    ssc.checkpoint("G:\\datas\\spark\\stream")

    val zkaddress = "hadoop001:2181,hadoop002:2181,hadoop003:2181"
    val groupId = "myId"
    //主题和对应使用的线程
    val topic = Map[String, Int]("wordcount" -> 1)

    //连接kafka,创建KafkaDStream
    val datas: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkaddress, groupId, topic)
    //获得DStream(Kafak的ReceiverInputDStream[(String, String)]里面装的是一个元组（key是写入的key，value是实际写入的内容)）
    val lines: DStream[String] = datas.map(_._2)

    val words: DStream[String] = lines.flatMap(_.split(" "))
    //实现累加使用updateStateByKey
    val reduced: DStream[(String, Int)] = words.map((_, 1))
      .updateStateByKey(KafkaFunUtils.updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)

    reduced.print()
    //启动sparksteaming程序
    ssc.start()
    //等待优雅的退出
    ssc.awaitTermination()
  }
}
