package rdd.demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 在spark中运行（去掉setMaster(**),放开读取和存储路径）
  * --executor-memory 2048mb：指定使用内存数; --total-executor-cores：指定使用计数核数
  * --master spark://hadoop001:7077：指定master地址;--class demo.WorldCount /data/sparkDemo.jar:指定main方法的类和存在于那个jar包
  * hdfs://hadoop001:9000/wordCount.txt hdfs://hadoop001:9000/wordCount:传入main的参数arg(0),arg(1)分别为hdfs中的路径
  * /data/spark-2.3.3# bin/spark-submit --master spark://hadoop001:7077  --executor-memory 2048mb --total-executor-cores 12 --class demo.WorldCount /data/sparkDemo.jar hdfs://hadoop001:9000/wordCount.txt hdfs://hadoop001:9000/wordCount
  */
object WorldCount {
  def main(args: Array[String]): Unit = {
    //创建spark配置，设置应用程序名字,设置本地运行以及默认的分区数
    //val conf = new SparkConf().setAppName("WorldCount")
    val conf = new SparkConf().setAppName("WorldCount").setMaster("local[2]")
    //创建spark执行的入口
    val sc = new SparkContext(conf)
    //指定以后从哪里读取数据创建RDD（弹性分布式数据集）
    //val lines: RDD[String] = sc.textFile(args(0))
    //创建算子(目标文件)
    val list = List("hello i am tom", "tom my name is lilei", "hi lilei i am lucy")
    val rdd = sc.parallelize(list)
    //切分压平
    val words = rdd.flatMap(_.split(" "))
    //将单词和一组合
    val wordAndOne = words.map((_, 1))
    //按key进行聚合
    val reduced = wordAndOne.reduceByKey(_ + _)
    //排序
    val sorted = reduced.sortBy(_._2, false)
    //打印输出到控制台
    sorted.collect().foreach(println)
    //将结果保存到目标文件中
    // sorted.saveAsTextFile(args(1))
    //释放资源
    sc.stop()
  }
}
