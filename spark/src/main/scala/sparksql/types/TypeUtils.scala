package sparksql.types

import scala.io.{BufferedSource, Source}

object TypeUtils {

  //定于方法，读取规则文件，转换成map格式
  def readRules(path: String): Map[String, String] = {
    val bf: BufferedSource = Source.fromFile(path)
    val lines: Iterator[String] = bf.getLines()
    //迭代器可以使用map，直接操作里面的每一个元素
    val map: Map[String, String] = lines.map(line => {
      val fields: Array[String] = line.split("\t")
      val key: String = fields(0)
      val value: String = fields(1)
      (key, value)
    }).toMap
    //返回map
    map
  }

  def main(args: Array[String]): Unit = {
    val tuples: Map[String, String] = readRules("G:\\datas\\type\\apptype_train.dat")
    val key: String = "140106"
    val value = tuples.get(key)
    //println(value)
  }
}