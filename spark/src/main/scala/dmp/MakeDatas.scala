package dmp

import java.io.File

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import rdd.iplocation.MyUtils


object MakeDatas {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println(
        """
          |1.ip path
          |2.log path
          |3.output dir
        """.stripMargin
      )
      sys.exit()
    }

    val session: SparkSession = SparkSession.builder().appName("MakeDatas")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[2]").getOrCreate()

    val sc = session.sparkContext

    //把Ip规则加载到driver端的内存中
    val tuples: Array[(Long, Long, String)] = MyUtils.readRules(args(0))

    //广播ip规则到executor中
    val broadRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(tuples)

    import session.implicits._
    //创建RDD，读取目标日志文件
    val rdd: Dataset[String] = session.read.textFile(args(1))
    val df: DataFrame = rdd.map(line => {
      val fields = line.split("[|]")
      val times = fields(0)
      val ip = fields(1) //获得ip数据
      val ipLong: Long = MyUtils.ip2Long(ip) //把ip转换成long格式
      val rulesInExecutor: Array[(Long, Long, String)] = broadRef.value //获得广播的规则变量
      val index: Int = MyUtils.binarySearch(rulesInExecutor, ipLong)
      var province: String = " : "
      if (index != -1) province = rulesInExecutor(index)._3 //复制查询到的省市

      val url = fields(2)
      (times, province.split("[:]")(0), province.split("[:]")(1), url)
    }).toDF("time", "province", "city", "url")

    df.show()
    //通过hadoop文件管理模块，递归删除存在的输出目录
    val configuration = sc.hadoopConfiguration
    val fs = FileSystem.get(configuration)
    val path = new Path(args(2))
    if (fs.exists(path)) {
      fs.delete(path, true)
    }

    df.write.parquet(args(2))
    sc.stop()
  }
}
