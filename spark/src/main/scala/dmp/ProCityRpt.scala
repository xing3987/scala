package dmp


import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

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
    val frame: DataFrame = session.sql("select province,city,count(*) count from datas group by province,city")
    frame.show()

    //使用hadoop的文件管理api，判断输出目录是否存在
    val hadoopConfiguration = session.sparkContext.hadoopConfiguration
    val fs: FileSystem = FileSystem.get(hadoopConfiguration)
    val path = new Path(args(1))
    if (fs.exists(path)) {
      fs.delete(path, true)
    }

    //保存到mysql
    //加载配置文件：application.conf ->application.json->application.properties
    val config = ConfigFactory.load()
    val properties = new Properties()
    properties.setProperty("user",config.getString("jdbc.user"))
    properties.setProperty("password",config.getString("jdbc.password"))
    //SaveMode保存格式，覆盖追加等
    frame.write.mode(SaveMode.Overwrite).jdbc(config.getString("jdbc.url"),"ip_city_count",properties)

    frame.coalesce(1).write.json(args(1)) //coalesce合并分区
    session.stop()
  }
}
