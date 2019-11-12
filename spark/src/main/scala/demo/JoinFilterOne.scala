package demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * detail.txt为用户注册信息全表，filter.txt为需要过滤掉的用户的手机号码
  * 要求detail.txt关联filter.txt并去除detail.txt中包含filter.txt中的用户
  * detail.txt数据
  * 张三 13007080388  man 山西 NULL 5  2015-11-05 21:22:41
  * 李四 13056677799  woman 四川 地推注册邀请 5  2015-10-11 11:32:19
  * 王五 13084470421  man 四川 地推注册邀请 6  2015-08-08 08:11:14
  * filter.txt数据
  * 13007080388
  * 13056677799
  * 思路
  * 1.分别加载两份数据
  * 2.解析数据并拼接成key为手机号的元组
  * 3.用户信息leftOuterJoin过滤表
  * 4.过滤掉不符合规则数据并输出
  */
object JoinFilterOne {
  def main(args: Array[String]) {
    //创建SparkConf()并设置App名称及Master地址
    val conf = new SparkConf().setAppName("JoinFilter").setMaster("local[1]")
    //创建SparkContext，该对象是提交spark App的入口
    val sc = new SparkContext(conf)
    //加载源数据
    val sourceAll = sc.textFile("S:\\logs\\detail.txt")
    val sourceFilter = sc.textFile("S:\\logs\\filter.txt")
    //转换RDD
    //(13007080388,(张三,13007080388,man,山西,NULL,5,2015-11-05 21:22:41)),
    // (13056677799,(李四,13056677799,woman,四川,地推注册邀请,5,2015-10-11 11:32:19)),
    // (13084470421,(王五,13084470421,man,四川,地推注册邀请,6,2015-08-08 08:11:14))
    val rddAll = sourceAll.map(x => {
      val line = x.split(" ")
      val name = line(0)
      val phone = line(1)
      val sex = line(2)
      val addr = line(3)
      val dataType = line(4)
      val dataCode = line(5)
      val dataDate = line(6)
      (phone, (name, phone, sex, addr, dataType, dataCode, dataDate))
    })
    //(13007080388,delete), (13056677799,delete)
    val rddFilter = sourceFilter.map(x => {
      val line = x.split(" ")
      val phone = line(0)
      (phone, "delete")
    })
    //(13056677799,张三,13056677799,woman,四川,地推注册邀请,5,2015-10-11 11:32:19,Some(delete)),
    // (13084470421,李四,13084470421,man,四川,地推注册邀请,6,2015-08-08 08:11:14,None),
    // (13007080388,王五,13007080388,man,山西,NULL,5,2015-11-05 21:22:41,Some(delete))

    rddAll.leftOuterJoin(rddFilter).map(x => {
      (x._1, x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5, x._2._1._6, x._2._1._7, x._2._2)
    }).filter(_._9 == None).saveAsTextFile("S:\\logs\\result")
    //println(result.toBuffer)
    //过滤后
    //(13084470421,王宇,13084470421,man,四川,地推注册邀请,6,2015-08-08 08:11:14,None)
    //停止sc，结束该任务
    sc.stop()
  }
}
