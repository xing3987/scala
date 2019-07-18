package bike

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * spark整合mongo
  */
object SparkMongo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("SparkMongo")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/bigdata.bike") // 指定mongodb输入
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/bigdata.bikeOut") // 指定mongodb输出
      .getOrCreate()

    // 加载数据
    val df: DataFrame = MongoSpark.load(spark)
    // 打印输出
    df.show
    df.createTempView("bike")

    spark.stop()
  }
}
