package sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/*
 *  spark1.1 sql
 */
object SQLDemo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SQLDemo1").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //spark1.1创建SparkSQL的连接（程序执行的入口）
    val sqlContext = new SQLContext(sc)

    //先有一个普通的RDD，然后在关联上schema，进而转成DataFrame
    val lines: RDD[String] = sc.textFile("G:\\datas\\input\\person\\*")

    val mapRDD: RDD[Boy] = lines.map(line => {
      val words: Array[String] = line.split(" ")
      val id = words(0).toLong
      val name = words(1)
      val age = words(2).toInt
      val pay = words(3).toDouble
      Boy(id, name, age, pay)
    })

    //该RDD装的是Boy类型的数据，有了schema信息，但是还是一个RDD
    //将RDD转换成DataFrame
    //导入隐式转换
    import sqlContext.implicits._
    val df: DataFrame = mapRDD.toDF
    //变成DF后可以使用两种API进行编程了
    //先把DataFrame先注册临时表
    df.registerTempTable("t_boy")
    //书写SQL查询
    val result: DataFrame = sqlContext.sql("select * from t_boy order by pay desc,age asc")
    //打印输出结果，触发action
    result.show()

    //println(mapRDD.collect.toBuffer)
    sc.stop()
  }
}

case class Boy(id: Long, name: String, age: Int, pay: Double)