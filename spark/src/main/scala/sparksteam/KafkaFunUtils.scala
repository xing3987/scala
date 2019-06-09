package sparksteam

object KafkaFunUtils {
  /**
    * 第一个参数：聚合的key,就是单词
    * 第二个参数：当前批次，该单词在每一个分区出现的次数
    * 第三个参数：初始值或累加的中间值
    */
  val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
    //iter.map(t => (t._1, t._2.sum + t._3.getOrElse(0)))
    iter.map { case (x, y, z) => (x, y.sum + z.getOrElse(0)) }
  }
}
