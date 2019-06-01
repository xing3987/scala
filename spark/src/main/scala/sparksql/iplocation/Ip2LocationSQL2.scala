package sparksql.iplocation

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import rdd.iplocation.MyUtils

/**
  * jon的代价太昂贵，而且非常慢。解决思路：将规则表缓存起来（广播变量）
  */
object Ip2LocationSQL2 {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().appName("Ip2LocationSQL")
      .master("local[2]").getOrCreate()

    import session.implicits._
    //获取ip规则并建表
    val lines: Dataset[String] = session.read.textFile(args(0))
    val rule: Dataset[(Long, Long, String)] = lines.map(line => {
      val fields: Array[String] = line.split("\\|")
      val start: Long = fields(2).toLong
      val end: Long = fields(3).toLong
      val province: String = fields(6) + ":" + fields(7)
      (start, end, province)
    })
    //收集数据到driver端
    val rules: Array[(Long, Long, String)] = rule.collect()
    val rulesRef: Broadcast[Array[(Long, Long, String)]] = session.sparkContext.broadcast(rules)


    //获取目标ip并建表
    val aims: Dataset[String] = session.read.textFile(args(1))
    val ddIP: DataFrame = aims.map(aim => {
      val ip: String = aim.split("\\|")(1)
      MyUtils.ip2Long(ip)
    }).toDF("ip")

    //分别创建视图并写sql->join查询
    ddIP.createTempView("aims")

    //定义一个自定义函数(UDF),并注册
    //该函数的功能是把ip转成一个省份，使用了从driver端广播的规则
    session.udf.register("ip2Province", (ipNum: Long) => {
      val getRules: Array[(Long, Long, String)] = rulesRef.value
      val index: Int = MyUtils.binarySearch(getRules, ipNum)
      var province = "未知"
      if (index != -1) {
        province = getRules(index)._3
      }
      province
    })

    //使用自定义udf查询得到结果(这样查就在每个executor上执行，不需要到其它机器上pull规则，不需要join大大加快了效率)
    val result: DataFrame = session.sql("select ip2Province(ip) province,count(0) as counts from aims" +
      " group by province order by counts desc")
    result.show()

    session.stop()
  }
}
