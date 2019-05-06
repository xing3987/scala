package demo

/**
  * object与class区别：不能new成对象
  * 含有默认初始化方法apply()
  */
object ValDemo1 {
  def main(args: Array[String]): Unit = {
    println("hello,this is my first scala")

    //变量的定义，可以使用var和val定义，val修饰的变量值不可以改变(final)
    var name = "tom"
    name = "toly"
    val name1: String = "marry"
    val name2 = "jerry"
    val s: Unit = 1; //Unit是表示没有，其值是一对括号()
    println(s, name, name1, name2) //可以打印多个数据，用逗号隔开

    var str = s"my name is $name, my sister name is $name1" //以s开头，可以使用$传参
    println(str)

    //var str1="hello my name is %s,I am come from china."
    printf("hello my name is %s,I am come from china.", "gogogo") //使用%s传参同python

    val i = 8
    var s2 = if (i > 8) i else 1;
    var s3 = if (s2 > 10) 10 else if (s2 == 8) s2
    println(s2, s3)

  }
}
