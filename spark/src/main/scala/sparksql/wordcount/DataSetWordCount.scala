package sparksql.wordcount

import org.apache.spark.sql._

/**
  * spark2.0 直接操作dataset->wordCount
  */
object DataSetWordCount {
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

    //导入聚合函数,直接操作dataset
    import org.apache.spark.sql.functions._
    val result: Dataset[Row] = flated.groupBy($"value" as "name")
      .agg(count("*") as "counts").orderBy($"counts" desc)
    result.show()

    session.stop()
  }
}
