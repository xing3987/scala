package sparksql.demo

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * spark join
  * 创建两张表执行join
  * Dataset当rdd操作需要导入隐式函数“import spark.implicits._”
  */
object JoinTest5 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("JoinTest5")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val lines: Dataset[String] = spark.createDataset(List("1,laozhoa,china", "2,laoduan,usa", "3,laoyang,jp"))
    //对数据进行整理

    val tpDs: Dataset[(Long, String, String)] = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val nationCode = fields(2)
      (id, name, nationCode)
    })
    val df1: DataFrame = tpDs.toDF("id", "name", "nation")

    val nations: Dataset[String] = spark.createDataset(List("china,中国", "usa,美国"))
    //对数据进行整理
    val ndataset: Dataset[(String, String)] = nations.map(l => {
      val fields = l.split(",")
      val ename = fields(0)
      val cname = fields(1)
      (ename, cname)
    })

    val df2: DataFrame = ndataset.toDF("ename", "cname")

    //第一种，创建视图
    /*
        df1.createTempView("user")
        df2.createTempView("country")
        val result: DataFrame = spark.sql("select * from user join country on user.nation=ename")
    */
    //第二种，直接操作DataFrame,使用lambda
    val result: DataFrame = df1.join(df2, $"nation" === $"ename")

    result.show()
    spark.stop()
  }
}
