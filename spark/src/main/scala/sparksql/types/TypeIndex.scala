package sparksql.types

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * spark给数据进行分类标注
  */
object TypeIndex {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().appName("SQLDemo4")
      .master("local[2]").getOrCreate()
    val sc: SparkContext = session.sparkContext

    //把规则加载到driver端的内存中
    val maps: Map[String, String] = TypeUtils.readRules(args(0))

    //广播ip规则到executor中
    val broadRef: Broadcast[Map[String, String]] = sc.broadcast(maps)

    //创建RDD，读取目标日志文件
    val rdd: RDD[String] = sc.textFile(args(1))

    val result = rdd.map(line => {
      val fields = line.split("\t")
      val id: String = fields(0)
      val types: String = fields(1)
      val type1 = types.split("[|]")(0)
      val maps: Map[String, String] = broadRef.value //获得广播的规则变量
      var value: String = maps(type1)
      var value2 = "";
      if (types.split("[|]").size > 1) {
        value2 = maps(types.split("[|]")(1))
      }
      //println(id, value)
      Row(id, value, value2)
    })

    //创建表的构造器
    val structType: StructType = StructType(List(
      StructField("id", StringType, true),
      StructField("lable1", StringType, true),
      StructField("lable2", StringType, true)
    ))

    //建立映射，将RowRDD关联schema
    val df: DataFrame = session.createDataFrame(result, structType)
    df.createTempView("results")
    df.show()

    /*val dfResult: DataFrame = sqlContext.sql("select * from results")
    dfResult.show()*/
    //println(result.collect().toBuffer)
    //df.repartition(1)
    //result.saveAsTextFile("S:\\other\\bigdata\\result")
    df.write.csv("G:\\datas\\type\\out")
    sc.stop()
  }
}