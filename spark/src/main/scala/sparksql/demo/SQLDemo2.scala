package sparksql.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark1.1 sql demo
  */
object SQLDemo2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SQLDemo2").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //spark1.1创建SparkSQL的连接（程序执行的入口）
    val sqlContext = new SQLContext(sc)

    //先有一个普通的RDD，然后在关联上schema，进而转成DataFrame
    val lines: RDD[String] = sc.textFile("G:\\datas\\input\\person\\*")

    //封装成默认的行键对象
    val mapRDD: RDD[Row] = lines.map(line => {
      val words: Array[String] = line.split(" ")
      val id = words(0).toLong
      val name = words(1)
      val age = words(2).toInt
      val pay = words(3).toDouble
      Row(id, name, age, pay)
    })

    //创建表头，用于描述DataFrame
    val structType: StructType = StructType(List(
      StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("pay", DoubleType, true)
    ))

    //建立映射，将RowRDD关联schema
    val df: DataFrame = sqlContext.createDataFrame(mapRDD,structType)

    //变成DF后可以使用两种API进行编程了
    //先把DataFrame先注册临时表
    df.registerTempTable("t_boy")
    //书写SQL查询
    val result: DataFrame = sqlContext.sql("select * from t_boy order by pay desc,age asc")
    //打印输出结果，触发action
    result.show()

    sc.stop()
  }
}
