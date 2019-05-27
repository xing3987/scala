package sort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 对数据进行排序
  * 先按收入，后按年龄
  * 使用对象的方式排序
  */
object CusSort1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    val arr = Array("Tom 30 9999", "Jerry 25 5000", "Kitty 18 30000", "Singer 40 5000", "docter 33 20000")
    val par: RDD[String] = sc.parallelize(arr)
    val tuples: RDD[User] = par.map(x => {
      val words: Array[String] = x.split(" ")
      val name = words(0)
      val age = words(1).toInt
      val pay = words(2).toInt
      new User(name, age, pay)
    })
    val sorted: RDD[User] = tuples.sortBy(u => u)
    println(sorted.collect.toBuffer)

    sc.stop()
  }
}

//排序的对象要进行网络传输，要实现序列化
class User(val name: String, val age: Int, val pay: Int) extends Ordered[User] with Serializable {
  override def compare(that: User): Int = {
    if (that.pay != this.pay) return that.pay - this.pay //收入大的在前面
    else return this.age - that.age //年龄小的在前面
  }

  override def toString: String = s"name:$name,age:$age,pay:$pay"

}

