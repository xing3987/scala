package bike

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


/**
  * 分析单车项目
  * 每天访问次数和日活
  */
object PVandUV {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("PVandUV")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/bigdata.log") // 指定mongodb输入
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/bigdata.logOut") // 指定mongodb输出
      .getOrCreate()
    // 加载数据
    val df: DataFrame = MongoSpark.load(spark)
    // 打印输出
    df.show
    df.createTempView("data")

    //过滤时间为年月日格式
    val data: DataFrame = spark.sql("select _id,latitude,longitude,openid,left(time,10) time from data")
    data.show()
    data.createTempView("datas")

    //求日访问量
    val pv: DataFrame = spark.sql("select count(*) pv,time from datas group by time")
    pv.show()
    //求日活
    val uv: DataFrame = spark.sql("select count(distinct(openid)) uv,time from datas group by time")
    uv.show()

    val result: DataFrame = spark.sql("select count(*) pv,count(distinct(openid)) uv,time from datas group by time")
    result.show()
    //把结果保存回mongodb
    MongoSpark.save(result)

    spark.stop()
  }
}
