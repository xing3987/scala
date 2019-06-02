package sparksql.demo

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 从各种数据源中读取数据
  * jdbc,csv,parquet
  */
object ReadDataSource8 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("JdbcDataSource7")
      .master("local[*]")
      .getOrCreate()

    //读取json数据,含有表头字段名
    //val df: DataFrame = spark.read.format("json").load("G:\\datas\\spark\\json")
    val df: DataFrame = spark.read.json("G:\\datas\\spark\\json")
    df.printSchema()
    df.show()

    //读取csv数据源,需要设置表头字段名（默认是_c0,_c1....）
    //val df1: DataFrame = spark.read.format("csv").load("G:\\datas\\spark\\csv")
    val df1: DataFrame = spark.read.csv("G:\\datas\\spark\\csv").toDF("id","province","count")
    df1.printSchema()
    df1.show()

    //读取parquet数据源
    val df2: DataFrame = spark.read.format("parquet").load("G:\\datas\\spark\\parquet")
    //val df2: DataFrame = spark.read.parquet("G:\\datas\\spark\\parquet")
    df2.printSchema()
    df2.show()


    spark.stop()
  }
}
