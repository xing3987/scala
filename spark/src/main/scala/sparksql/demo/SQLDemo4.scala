package sparksql.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * spark2.0使用新的api sparkSession
  */
object SQLDemo4 {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().appName("SQLDemo4")
      .master("local[2]").getOrCreate()
    val lines: RDD[String] = session.sparkContext.textFile("G:\\logs\\person.txt")
    val maped: RDD[Row] = lines.map(line => {
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

    val df: DataFrame = session.createDataFrame(maped,struct)
    df.show()

    //对数据操作要导入隐式函数
    import session.implicits._
    //两种方法都可以
    //val df2: Dataset[Row] = df.where($"age">10).sort($"pay" desc,$"age" asc)
    val df2: Dataset[Row] = df.where($"age">10).orderBy($"pay" desc,$"age" asc)
    df2.show()

    session.stop()
  }
}
