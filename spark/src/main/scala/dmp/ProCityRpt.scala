package dmp


import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 统计各个省市的数据数量
  */
object ProCityRpt {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println(
        """
          |1.ip path
          |2.output dir
        """.stripMargin
      )
      sys.exit()
    }

    val session: SparkSession = SparkSession.builder().appName("MakeDatas")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[2]").getOrCreate()

    val df: DataFrame = session.read.parquet(args(0))
    df.createTempView("datas")
    val frame: DataFrame = session.sql("select province,city,count(*) from datas group by province,city")
    frame.show()

    //使用hadoop的文件管理api，判断输出目录是否存在
    val hadoopConfiguration = session.sparkContext.hadoopConfiguration
    val fs: FileSystem = FileSystem.get(hadoopConfiguration)
    val path = new Path(args(1))
    if (fs.exists(path)) {
      fs.delete(path, true)
    }

    frame.coalesce(1).write.json(args(1)) //coalesce合并分区
    session.stop()
  }
}
