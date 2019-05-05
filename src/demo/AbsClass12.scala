package demo

/**
  * 抽象类，同java,抽象类只能使用extends，但是trait可以使用with
  */
abstract class AbsClass12 {
  def go() = {
    println("gogogogo..")
  }

  def back()
}

object AbsImpl extends AbsClass12 with TraitDemo11{
  override def back(): Unit = {}

  override def sayhello(): Unit = {}
}
