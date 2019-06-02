package sparksql.demo

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * 读取jdbc数据源
  */
object JdbcDataSource7 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("JdbcDataSource7")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val df: DataFrame = spark.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://localhost:3306/bigdata",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "ip_province_count",
        "user" -> "root",
        "password" -> "root")
    ).load()
    df.show()

    /*
        //过滤row中第二个数据小于1000的
        val filted: Dataset[Row] = df.filter(r => {
          r.getAs[Int](2) <= 1000
        })
        filted.show()*/
    val filted: Dataset[Row] = df.filter($"count" <= 1000)
    filted.show()

    //查询
    val result: DataFrame = df.select($"id", $"province", $"count" * 10 as "count")
    result.show()
    //或者注册成视图，然后用sql操作
/*
    //输出到mysql
    val pro = new Properties()
    pro.put("user","root")
    pro.put("password","root")
    //overwrite：覆盖,append：追加,ignore：没有就创建，有就忽略,error：如存在表报错
    result.write.mode("overwrite").jdbc("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8","ip_province_result",pro)

    //输出成text,只能有一列数据
    result.write.text("G:\\datas\\spark\\text")
*/
    //输出成json文件
    result.write.json("G:\\datas\\spark\\json")

    //输出成csv文件
    result.write.csv("G:\\datas\\spark\\csv")

    //输出成parquet文件,保存有表头信息和数据信息，并压缩
    result.write.parquet("G:\\datas\\spark\\parquet")

    spark.stop()
  }
}
