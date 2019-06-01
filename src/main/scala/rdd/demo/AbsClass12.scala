package rdd.demo

/**
  * 抽象类，同java,抽象类只能使用extends，但是trait可以使用with
  */
abstract class AbsClass12 {
  final def go() = {  //方法加了final不能被重写，变量加final也不能重写,final修饰的类不能继承
    println("gogogogo..")
  }

  def back()
}

object AbsImpl extends AbsClass12 with TraitDemo11{
  override def back(): Unit = {}

  override def sayhello(): Unit = {}

  override val name: String = "Marry"
}
