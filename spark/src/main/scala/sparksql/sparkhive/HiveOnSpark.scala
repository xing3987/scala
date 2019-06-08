package sparksql.sparkhive

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * spark 整合hive
  * 1.pom添加spark-hive_2.11依赖
  * 2.导入集群中的core-site.xml hdfs-site.xml hive-site.xml文件到resources中
  * 3.sparkSession 使用 enableHiveSupport() 建立依赖
  */
object HiveOnSpark {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("HiveOnSpark")
      .master("local[2]").enableHiveSupport().getOrCreate()

    val df: DataFrame = spark.sql("select * from access")
    df.show()

    //spark.sql("create table t_boy(id long,name string,age int)")
    val df2: DataFrame = spark.sql("desc t_boy")
    df2.show()

    spark.stop()
  }
}
