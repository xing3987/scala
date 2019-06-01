package rdd.iplocation

import java.sql.{Connection, DriverManager, PreparedStatement}

import scala.io.{BufferedSource, Source}

object MyUtils {

  //定于方法，读取ip规则文件，得到ip对应十进制起始到结束的区间，对应省市
  def readRules(path: String): Array[(Long, Long, String)] = {
    val bf: BufferedSource = Source.fromFile(path)
    val lines: Iterator[String] = bf.getLines()
    //迭代器可以使用map，直接操作里面的每一个元素
    val array: Array[(Long, Long, String)] = lines.map(line => {
      val fields: Array[String] = line.split("\\|")
      val start: Long = fields(2).toLong
      val end: Long = fields(3).toLong
      val province: String = fields(6) + ":" + fields(7)
      (start, end, province)
    }).toArray
    //返回数组
    array
  }

  //定义将ip转化成long的规则:
  def ip2Long(ip: String): Long = {
    val fragment: Array[String] = ip.split("\\.") //ip.split("[.]")
    var ipNum = 0L
    for (i <- fragment) {
      /*
      ipNum = ipNum << 8 //规则值右移一个字节即：乘以2的8次方->256
      ipNum += i.toLong //再加上每个ip数字
      //第一位*256*256*256+第二位*256*256+第三为*256+第四位
      */
      ipNum = ipNum << 8 | i.toLong
    }
    ipNum
  }

  //定义查询方法(二分法),传递ip和ip规则范围，返回规则中数据的角标，没有就返回-1
  def binarySearch(lines: Array[(Long, Long, String)], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    var middle = 0
    while (low <= high) {
      middle = (low + high) / 2
      if (ip < lines(middle)._1) high = middle - 1
      else if (ip > lines(middle)._2) low = middle + 1
      else return middle
    }
    -1
  }

  //定义插入数据库的方法
  def data2Mysql(pa: Iterator[(String, Int)]) = {
    val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8", "root", "root")
    val pstm: PreparedStatement = conn.prepareStatement("insert into ip_province_count(province,count) values(?,?)")
    pa.foreach(tp => {
      pstm.setString(1, tp._1)
      pstm.setInt(2, tp._2)
      pstm.executeUpdate()
    })
    if (pstm != null) pstm.close()
    if (conn != null) conn.close()
  }

  def main(args: Array[String]): Unit = {
    val tuples: Array[(Long, Long, String)] = readRules("G:\\datas\\ip\\ip.txt")
    val ip = ip2Long("1.13.128.1")
    val index: Int = binarySearch(tuples, ip)
    if (index != -1) println(tuples(index))
  }
}
