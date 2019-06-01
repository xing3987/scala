package rdd.demo

/**
  * scala的方法和变量都是静态的
  * 它还有个默认的方法apply
  */
object ObjectDemo8 {

  def main(args: Array[String]): Unit = {
    ObjectDemo8("hello")

    val t1 = new Teacher9("xiaoming", 1)
    println(t1.age, t1.sex)
    val t2 = new Teacher9("xiaohong", 2, "female")
    println(t2.sex, t2.age)

    val s1 = new Student10("hello")
    val s2 = Student10("xiaohong", "male")
    println(s1, s2)
  }

  def apply(x: String): Unit = {
    println("重写默认的apply方法,输入参数：" + x)
  }
}


