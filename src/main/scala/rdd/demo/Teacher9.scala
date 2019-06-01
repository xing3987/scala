package rdd.demo

/**
  * 定义一个类，有个空参数构造方法
  * 定义一个私有的类private class,只能在当前包下才可以访问(java->protect)，子包也不可以调用
  *   可以用pvivate[package] class开发包的访问权限
  * 定义在类名称后面的构造器叫主构造器,主构造器中的属性会默认定义成成员变量
  * 如果主构造器中的属性没有var或者val则是private的不能被外部直接调用
  * var : 相当于提供了get set方法
  * val ： 相当与提供了get方法
  */
class Teacher9(name: String, var age: Int) {
  //var name1: String = _  //定义一个变量默认值为null
  //var age1: Int = _
  var sex: String = _

  //定义一个辅助构造器，this()
  def this(name: String, age: Int, sex: String) = {
    this(name, age) //先调用主构造器
    this.sex = sex
  }
}

