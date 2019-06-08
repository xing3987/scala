package sparksteam

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 第一个steam程序，wordcount
  */
object SteamingWordCount1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SteamingWordCount1").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    //实时计算，使用StreamingContext,并要指定隔多长时间处理一批数据
    val ssc = new StreamingContext(sc, Milliseconds(5000))

    //有了StreamingContext,就可以创建sparkStreaming的抽象
    //从一个socket端口中读取数据(这里使用本机java创建了一个socket端口写出数据)
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.2.200", 9999)
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val wordAndOne: DStream[(String, Int)] = words.map((_, 1))
    val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey(_ + _)
    reduced.print()

    //启动sparksteaming程序
    ssc.start()
    //等待优雅的退出
    ssc.awaitTermination()
  }
}
