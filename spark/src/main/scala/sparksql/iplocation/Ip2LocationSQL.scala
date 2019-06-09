package sparksql.iplocation

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import rdd.iplocation.MyUtils

/**
  * 使用spark-sql的方式查找ip的地理位置
  * 建两张df视图，使用sql->join
  */
object Ip2LocationSQL {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().appName("Ip2LocationSQL")
      .master("local[2]").getOrCreate()

    import session.implicits._
    //获取ip规则并建表
    val lines: Dataset[String] = session.read.textFile(args(0))
    val rule: DataFrame = lines.map(line => {
      val fields: Array[String] = line.split("\\|")
      val start: Long = fields(2).toLong
      val end: Long = fields(3).toLong
      val province: String = fields(6) + ":" + fields(7)
      (start, end, province)
    }).toDF("start", "end", "province")

    //获取目标ip并建表
    val aims: Dataset[String] = session.read.textFile(args(1))
    val ddIP: DataFrame = aims.map(aim => {
      val ip: String = aim.split("\\|")(1)
      MyUtils.ip2Long(ip)
    }).toDF("data/ip")

    //分别创建视图并写sql->join查询
    rule.createTempView("rule")
    ddIP.createTempView("aims")

    val result: DataFrame = session.sql("select province,count(0) as counts from rule" +
      " join aims on (aims.ip>=rule.start and aims.ip<=rule.end) " +
      "group by province order by counts desc")
    result.show()

    session.stop()
  }
}
