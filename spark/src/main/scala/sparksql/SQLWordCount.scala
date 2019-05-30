package sparksql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * spark2.0 构建视图使用sql->wordCount
  */
object SQLWordCount {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().appName("SQLWordCount")
      .master("local[2]").getOrCreate()
    //Dataset分布式数据集，是对RDD的进一步封装，是更加智能的RDD,直接就转成了表状结构
    //使用dataset保存数据，默认只有一列，列名为“value”
    val lines: Dataset[String] = session.read.textFile("G:\\logs\\stjoin.txt")
    //操作dataset要导入session的隐式函数
    import session.implicits._
    val flated: Dataset[String] = lines.flatMap(_.split(" "))
    flated.show()

    //注册视图
    flated.createTempView("words")
    val result: DataFrame = session.sql("select value name,count(0) counts from words" +
      " group by value order by counts desc")
    result.show()

    session.stop()
  }
}
