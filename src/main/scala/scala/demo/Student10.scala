package scala.demo

/**
  * 定义一个私有的类,只能在当前包下才可以访问(java->protect)
  * private 变量表示不能被外部调用:相当与没有get set方法
  * 也适用于方法和辅助构造器
  */
class Student10(private var name: String) {

  var sex: String = _

  private def this(name: String,sex: String) = {
    this(name) //先调用主构造器
    this.sex = sex
  }

  def go(): Unit ={
    println("go go go..")
  }
}

/**
  * 同名object叫类的伴生对象
  * 伴生对象可以访问类的私有变量和方法
  * 可以重写apply方法用于初始化对象
  */
object Student10{

  //使用伴生对象的apply方法，用于初始化对象,注意返回类型
  def apply(name:String,sex:String) ={
    new Student10(name,sex)
  }

  def main(args: Array[String]): Unit = {
    new Student10("xiaoming").go()

    val s1 = new Student10("hello")
    println(s1.name,s1.sex)
    val s2 = Student10("xiaohong", "male")
    println(s2.sex,s1.name)
  }
}
