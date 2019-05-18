package demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * action方法，会立即发task执行,在worker(executor)端执行(结果也在executor中),如下
  * aggregate
  * collect
  * saveAsTextFile
  * foreach
  * foreachPartiton
  *
  * 其余方法是transformation方法
  *   特点：lazy的，生成新的RDD或RDDPair,不会立即执行得到结果
  */
object SparkRDD {
  //指定分区的方式local[n],n是几，分区就是几个。local[*]表示通过cpu核数得到相同的分区数
  //sc.parallelize(list，n)n是几，分区就是几个
  val conf = new SparkConf().setAppName("SparkRDD").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val list = List("tom", "davy", "lucy", "tom")
  val list1 = List(1, 2, 3, 4, 5)
  val list2 = List(3, 4, 5, 6)
  implicit val rdd = sc.parallelize(list)
  val rdd1 = sc.parallelize(list1)
  val rdd2 = sc.parallelize(list2)

  def main(args: Array[String]): Unit = {
    /*mapdm
    flatMapdm
    flatMapValuesdm
    mapPartitionsdm
    mapPartitionsWithIndexdm

    reducedm
    reduceByKeydm

    uniondm
    groupByKeydm

    joindm
    cogroupdm

    sampledm
    cartesiandm

    filterdm
    filterByRangedm
    distinctdm
    intersectiondm

    coalescedm
    repartitiondm
    repartitionAndSortWithinPartitionsdm

    sortByKeydn
    aggregatedm


    rdd.top(3).foreach(println) //排序取最前三个
    rdd.take(3).foreach(println) //取list的前三个
    rdd.takeOrdered(3).foreach(println) //排序取最前三个
    aggregateByKeydm

    countBy
    foldByKeydm*/

  }

  //foldByKey折叠聚合，与reduceByKey不同的是它含有初始值
  //foldByKey,reduceByKey,combineByKey,aggregateByKey底层都是调用combineByKeyWithClassTag
  def foldByKeydm = {
    val list5 = List((1, "a"), (2, "b"), (3, "c"), (1, "d"))
    val rdd5 = sc.parallelize(list5)
    //foldByKeydm
    rdd5.foldByKey("")(_+_).foreach(println)
  }

  //countBy求总数
  def countBy = {
    val list5 = List((1, "a"), (2, "b"), (3, "c"), (1, "d"))
    val rdd5 = sc.parallelize(list5)
    //整体相同的个数(1, "a")->1个，(2, "b")->1个..
    rdd5.countByValue().foreach(println)
    //key相同的个数1->2个，2->1个..
    rdd5.countByKey().foreach(println)
  }

  //aggregate对rdd聚合操作
  def aggregatedm = {
    //（1，2，3）->1+2+3=6  (4,5)->4+5=9  6+9=15
    var i = rdd1.aggregate(0)(_ + _, _ + _)
    println(i)
    //多个分区，每个分区内部聚合使用一次初始值，分区间聚合时又使用一次初始值
    //（1，1，2，3）->1+1+2+3=7  (1,4,5)->1+4+5=10  1+7+10=18
    i = rdd1.aggregate(1)(_ + _, _ + _)
    println(i)
    //求每个分区的最大值再求和
    //（1，2，3）->3  (4,5)->5  3+5=8
    i = rdd1.aggregate(0)(math.max(_, _), _ + _)
    println(i)
    //求每个分区的最大值再求和,有初始值时，分区内操作会使用，分区间操作也会使用
    //（4，1，2，3）->4  (4,4,5)->5  4+4+5=13
    i = rdd1.aggregate(4)(math.max(_, _), _ + _)
    println(i)

    val str = rdd.aggregate("")(_ + _, _ + _)
    println(str)
    //多个分区，每个分区内部聚合使用一次初始值，分区间聚合时又使用一次初始值
    val str1 = rdd.aggregate("|")(_ + _, _ + _)
    println(str1)
  }

  //aggregateByKey函数对PairRDD中相同Key的值进行聚合操作，在聚合过程中同样使用了一个中立的初始值
  //第一个_+_为本分区内的聚合操作，初始值为第一个_的值
  //第二个_+_为分区间的聚合操作，初始值不会被使用
  //如果分区数>1时，初始值就被使用了多次（每个分区内）
  def aggregateByKeydm = {
    //分两个区：[("tom"，1),("davy"，1)],[("lucy",1),("tom",1)]
    //[("tom"，1+1=2),("davy"，1+1=2)],[("lucy",1+1=2),("tom",1+1=2)]
    //[("tom"，2+2=4),("davy"，2),("lucy",2)]
    rdd.map((_, 1)).aggregateByKey(1)(_ + _, _ + _).foreach(println)
  }

  //sortByKey函数作用于Key-Value形式的RDD
  def sortByKeydn = {
    //zip也可以用于rdd操作，但是与scala-zip不同的是两个rdd长度要是一致的
    //传参true：升序，false:降序
    rdd.zip(rdd2).sortByKey(true).foreach(println)
  }

  /*
   * repartitionAndSortWithinPartitions函数是repartition函数的变种，与repartition函数不同的是，
   * repartitionAndSortWithinPartitions在给定的partitioner内部进行排序，性能比repartition要高。
   * 只对map的list操作，需要转递一个分区方法
   */
  def repartitionAndSortWithinPartitionsdm = {
    rdd1.map(x => (x, x)).repartitionAndSortWithinPartitions(new HashPartitioner(2)) //传递分区方法为hash值，分区数2
      .mapPartitionsWithIndex((index, iterable) => {
      var list = ListBuffer[String]()
      while (iterable.hasNext) {
        var str: String = "partition:" + index + "," + iterable.next()
        list = list :+ str
      }
      list.toIterator
    }).foreach(println)
  }

  //repartition进行重分区，解决的问题：本来分区数少  -》 增加分区数
  def repartitiondm = {
    rdd1.repartition(4).foreach(println)
  }

  //coalesce重新分区，分区数由多变少
  def coalescedm = {
    rdd1.coalesce(1).foreach(println)
  }

  //intersection求两个rdd的交集
  def intersectiondm = {
    rdd1.intersection(rdd2).foreach(println)
  }

  //distinct去重复
  def distinctdm() = {
    rdd.distinct().foreach(x => print(x + " "))
  }

  //filter对数据进行过滤
  def filterdm = {
    //rdd1.filter(_ % 2 == 0).foreach(print)
    rdd1.filter(x => x % 2 == 0).foreach(print)
  }

  //filterByRange通过key的范围对数据进行过滤
  def filterByRangedm = {
    val list5 = List((1, "a"), (2, "b"), (3, "c"), (1, "d"))
    val rdd5 = sc.parallelize(list5)
    rdd5.filterByRange(1, 2).foreach(print)
  }

  //cartesian是用于求笛卡尔积的
  def cartesiandm = {
    rdd.cartesian(rdd1).foreach(println)
  }

  /**
    * sample用来从RDD中抽取样本。他有三个参数
    * withReplacement: Boolean,
    * true: 有放回的抽样
    * false: 无放回抽象
    * fraction: Double：抽取样本的比例
    * seed: Long：随机种子
    */
  def sampledm = {
    val list5 = 1 to 100
    val rdd5 = sc.parallelize(list5)
    rdd5.sample(false, 0.1, 0).foreach(x => print(x + " "))
  }

  //groupByKey是将PairRDD中拥有相同key值得元素归为一组
  def groupByKeydm = {
    val list5 = List((1, "a"), (2, "b"), (3, "c"), (1, "d"))
    val rdd5 = sc.parallelize(list5)
    rdd5.groupByKey().foreach(t => {
      val iterator = t._2.iterator
      var value: String = ""
      while (iterator.hasNext) {
        value += iterator.next() + " "
      }
      println(t._1, value)
    })
  }

  //join是将两个PairRDD合并，并将有相同key的元素分为一组，可以理解为groupByKey和Union的结合
  def joindm = {
    //join和scala-zip区别
    val list5 = List((1, "a"), (2, "b"), (3, "c"))
    val list6 = List((2, "x"), (1, "y"))
    list5.zip(list6).foreach(println)
    val rdd5 = sc.parallelize(list5)
    val rdd6 = sc.parallelize(list6)
    rdd5.join(rdd6).foreach(println)
  }

  /*
   * 1.对两个RDD中的KV元素，每个RDD中相同key中的元素分别聚合成一个集合。
   * 2.与reduceByKey不同的是针对两个RDD中相同的key的元素进行合并
   * 3.cogroup相当于SQL中的全外关联full outer join，返回左右RDD中的记录，关联不上的为空。
   * 4.join相当于SQL中的内关联join，只返回两个RDD根据K可以关联上的结果，join只能用于两个RDD之间的关联，
   *      如果要多个RDD关联，多关联几次即可。
   */
  def cogroupdm = {
    val list5 = List((1, "a"), (2, "b"), (3, "c"))
    val list6 = List((2, "x"), (1, "y"))
    val rdd5 = sc.parallelize(list5)
    val rdd6 = sc.parallelize(list6)
    rdd5.cogroup(rdd6).foreach(println)
  }

  def uniondm = {
    //两个RDD合并
    rdd1.union(rdd2).foreach(println)
  }

  //reduceByKey仅将RDD中所有K,V对中K值相同的V进行合并
  def reduceByKeydm = {
    val maprs = rdd.map(x => (x, 1)).reduceByKey((_ + _))
    maprs.foreach(println)
  }

  //reduce其实是讲RDD中的所有元素进行合并
  def reducedm = {
    val str = rdd.reduce((x, y) => x + y)
    //val str = rdd.reduce((_ + _))
    println(str)

  }

  //对rdd中的元素进行分别处理 并且可以对不同分区进行操作index为分区号
  def mapPartitionsWithIndexdm[T](implicit rdd: RDD[T]) = {
    val rdd2 = rdd.mapPartitionsWithIndex((index, iterable) => {
      var list = ListBuffer[String]()
      while (iterable.hasNext) {
        var str: String = "partition:" + index + "," + iterable.next()
        if (str == "davy") {
          str = str + "001"
        }
        list = list :+ str
      }
      list.toIterator
    })
    rdd2.foreach(println)
  }

  def mapdm[T](implicit rdd: RDD[T]) = {
    //map
    val maps = rdd.map(x => (x, 1))
    maps.foreach(println)
  }

  def flatMapdm[T](implicit rdd: RDD[T]) = {
    //flatMap
    val flatMaps = rdd.flatMap(line => (line + " Hello").split(" "))
    flatMaps.foreach(println)
  }

  def flatMapValuesdm[T](implicit rdd: RDD[T]) = {
    val list5 = List((1, "a b"), (2, "b c"), (3, "c d"), (1, "d e"))
    val rdd5 = sc.parallelize(list5)
    //flatMapValues对value分割
    val flatMaps = rdd5.flatMapValues(line => line.split(" "))//->(1, "a b")-> (1,a),(1,b)
    flatMaps.foreach(println)
  }

  def mapPartitionsdm[T](implicit rdd: RDD[T]) = {
    //mapPartitions 对rdd中的元素进行分别处理
    val rdd2 = rdd.mapPartitions(iterable => {
      var list = ListBuffer[String]()
      while (iterable.hasNext) {
        var str: String = iterable.next().toString
        if (str == "davy") { //eq是对象的对比，不能使用
          str = str + "001"
        }
        list = list :+ str
      }
      println(list)
      list.toIterator
    })
    rdd2.foreach(println)
  }

}
