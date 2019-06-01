package rdd.demo

/**
  * 特质，相当与interface
  * 不同的是：trait可以有实现的方法，也可以定义没有实现的方法
  */
trait TraitDemo11 {

  val name: String //trait可以定义变量，不给值，继承是必须重写

  def sayhello(): Unit

  def sayBye(name: String): Unit = {
    println(s"say bye to $name")
  }
}

/**
  * 重写或者实现方法或者变量使用关键字override
  */
object TraitImpl extends TraitDemo11 {

  override val name: String = "tom" //只有val的变量才能重写

  override def sayhello(): Unit = {
    //如果父类没有方法体，可以不加override，如果有方法体必须使用关键字
    println("hello everybody~~")
  }

  def main(args: Array[String]): Unit = {
    sayhello()
    sayBye(name)

    //特质的动态混用,使用关键子with + 需要的特质
    //特制可以动态混用多个，末尾使用: with+特质+with+特质
    //也可以在定义的时候在类后面: with+特质
    val student10 = new Student10("tom") with TraitDemo11 {
      override def sayhello(): Unit = {}
      override val name: String = "jim"
    }

    student10.sayBye(student10.name)
  }
}