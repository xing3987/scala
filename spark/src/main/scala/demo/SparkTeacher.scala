package demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 求最受欢迎的老师，多个task要先collect到driver上再输出
  * 1.在所有的老师中求出最受欢饮的老师Top3
  * 2.求每个学科中最受欢迎老师的top3
  */
object SparkTeacher {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkTeacher").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("G:\\logs\\teacher.log")

    //1.在所有的老师中求出最受欢饮的老师Top3, 先collect到driver上再输出,take(n)去前几个结果(.reverse.take(n)去最后几个)
    rdd.map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false).collect.take(3).foreach(x => {
      println("老师：" + x._1.split(".cn/")(1) + ",欢迎度：" + x._2)
    })

    //2.求每个学科中最受欢迎老师的top3 用.分割要转义"\\"或者"[.]"
    val maps: RDD[((String, String), Int)] = rdd.map(x => ((x.split("//")(1).split("\\.")(0), x.split("//")(1).split("/")(1)), 1))
    //分组聚合
    val groups: RDD[(String, Iterable[((String, String), Int)])] = maps.reduceByKey(_ + _).groupBy(_._1._1)
    //组内求topn，mapValues对值进行操作，x => x.toList.sortBy(_._2).reverse.take(3)为scala的方法
    val topn: RDD[(String, List[((String, String), Int)])] = groups.mapValues(x => x.toList.sortBy(_._2).reverse.take(3))
    //打印输出结果
    //topn.foreach(println)
    topn.foreach(x => {
      var it = x._2.iterator
      while (it.hasNext) {
        val x2 = it.next()
        print(x._1 + ":" + x2._1._2 + "->" + x2._2 + ";")
      }
      println
    })

    //3.求每个学科中最受欢迎老师的top3，当数据量大时x.toList.sortBy(_._2).reverse.take(3)为scala的方法，
    //    scala的方法会把数据都放到内存中，可能导致内存溢出，所以要使用sparkrdd的reduce+filter

    //合并数据
    val reduced: RDD[((String, String), Int)] = rdd.map(x => ((x.split("//")(1).split("\\.")(0), x.split("//")(1).split("/")(1)), 1)).reduceByKey(_ + _)
    //    reduced.filter(_._1._1 == "bigdata").sortBy(_._2, false).take(3).foreach(println) //求bigdata最受欢迎的老师
    //求出所有的学科
    val subjects: Array[String] = rdd.map(_.split("//")(1).split("\\.")(0)).distinct().collect
    //分别过滤求每个学科最受欢迎的top3 多个学科多次shuffle,多次计算，可以把数据放到内存中，加快运行速度
    val cached: RDD[((String, String), Int)] = reduced.cache()
    /*
    //或者指定储存方式
    reduced.persist(StorageLevel.DISK_ONLY) //默认只放在内存中
    StorageLevel.DISK_ONLY 放于磁盘中
    StorageLevel.DISK_ONLY_2 放于磁盘中并存两份
    StorageLevel.MEMORY_ONLY
    StorageLevel.MEMORY_ONLY_2
    StorageLevel.MEMORY_ONLY_SER 序列化放于内存中
    StorageLevel.MEMORY_ONLY_SER_2
    StorageLevel.MEMORY_AND_DISK
    StorageLevel.MEMORY_AND_DISK_2
    StorageLevel.MEMORY_AND_DISK_SER
    StorageLevel.MEMORY_AND_DISK_SER_2
    */
    for (subject <- subjects) {
      //reduced.filter(_._1._1 == subject).sortBy(_._2, false).take(3).foreach(println)
      cached.filter(_._1._1 == subject).sortBy(_._2, false).take(3).foreach(println)
    }
    cached.unpersist() //释放缓存

 /*
    //或者使用checkpoint保存中间结果到磁盘中,以后就可以从备份的数据开始计算
    sc.setCheckpointDir("G:\\logs\\teacher_checkpoint")
    reduced.checkpoint()
    for (subject <- subjects) {
      reduced.filter(_._1._1 == subject).sortBy(_._2, false).take(3).foreach(println)
    }
*/
    //有cache时优先从cache中读取数据，没有才从checkpoint中读取，所以cache优先级别更高！！


    //方法2和3对比
    //当数据量小时，使用方法2，只需要一次action就可以得到结论；当数据量大时使用方法3，内存不会溢出

    //4.使用自定义分区器
    val subPa = new SubjectParitioner(subjects)
    val partitioned: RDD[((String, String), Int)] = reduced.partitionBy(subPa)
    //如果一次拿出一个分区(可以操作一个分区中的数据了)
    val sorted: RDD[((String, String), Int)] = partitioned.mapPartitions(it => {
      //将迭代器转换成list，然后排序，在转换成迭代器返回
      it.toList.sortBy(_._2).reverse.take(3).iterator
    })
    val r: Array[((String, String), Int)] = sorted.collect()
    println(r.toBuffer)

    //5.直接在reduce阶段使用自定义分区器
    val subReduced: RDD[((String, String), Int)] = rdd.map(x => ((x.split("//")(1).split("\\.")(0), x.split("//")(1).split("/")(1)), 1)).reduceByKey(subPa, _ + _)
    val subSorted: RDD[((String, String), Int)] = subReduced.mapPartitions(it => {
      //将迭代器转换成list，然后排序，在转换成迭代器返回
      it.toList.sortBy(_._2).reverse.take(3).iterator

      //为了不一次加载到内存，也可以使用长度为5的一个可以排序的集合，然后一个个it.next()取值对比
    })
    val results: Array[((String, String), Int)] = subSorted.collect()
    println(results.toBuffer)

    sc.stop()
  }
}

class SubjectParitioner(subjects: Array[String]) extends Partitioner {
  //初始化subject对应的分区号
  val rules = new mutable.HashMap[String, Int]()
  var i = 0
  for (subject <- subjects) {
    rules.put(subject, i)
    i = i + 1
  }

  //返回分区总数
  override def numPartitions: Int = subjects.length

  override def getPartition(key: Any): Int = {
    //获取学科名称 因为已知传来的key是(String,String)类型的，所以使用强转
    val subject = key.asInstanceOf[(String, String)]._1
    //根据规则计算分区编号
    rules(subject)
  }

}
