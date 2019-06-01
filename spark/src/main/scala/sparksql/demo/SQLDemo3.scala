package sparksql.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark1.1 直接使用DataFrame查询
  */
object SQLDemo3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SQLDemo3").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

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

    val struct: StructType = StructType(List(
      StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("pay", DoubleType, true)
    ))

    val df: DataFrame = sqlContext.createDataFrame(mapRDD, struct)
    df.show()

    //不使用SQL的方式，就不用注册临时表了
    val df1: DataFrame = df.select("name", "age", "pay")
    df1.show()

    //使用查询，导入隐式类
    import sqlContext.implicits._
    val df2: Dataset[Row] = df1.orderBy($"pay" desc, $"age" asc)
    df2.show()

    sc.stop()
  }
}
