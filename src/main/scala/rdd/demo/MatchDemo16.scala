package rdd.demo

/**
  * 模式匹配，匹配顺序是从上到下一次匹配
  * 如果所有的都匹配不上会报错，所以在case最后加 case _通配条件
  */
object MatchDemo16 {
  def main(args: Array[String]): Unit = {

    matchstr("dog")
    matchstr("cat")
    matchstr("hello")
    Helper.printlnSplit("str")

    matchtype("hello")
    matchtype(123)
    matchtype(Array(1, 2, 3))
    Helper.printlnSplit("type")

    matcharray(Array(0))
    matcharray(Array(0, 1))
    matcharray(Array(1, "hello", 3))
    matcharray(Array(8, 4, 'a'))
    matcharray(0)
    Helper.printlnSplit("array")

    matchlist(List(0))
    matchlist(List(7, 9))
    matchlist(List(1, 3, 4))
    matchlist(List(9999)) //只有一个元素也可以匹配拥有头和尾，只不过是空的
    matchlist(List(5, "a"))
    matchlist(1)
    Helper.printlnSplit("list")

    matchtuple((0, 1))
    matchtuple((1, 2, 3))
    matchtuple(("dog", "cat"))
    matchtuple((2, "x"))
    Helper.printlnSplit("tuple")

    //必须使用case class 才能用于模式匹配
    matchobj(OneCaseObj("tom"))
    matchobj(TwoCaseObj("tom", "xx"))
    matchobj(ThreeCaseObj)
    matchobj(0)
  }

  //匹配字符串
  def matchstr(str: String) = str match {
    case "dog" => println("hello dog..")
    case "cat" => println("miao..")
    case _ => println("can not match..")
  }

  //匹配数据类型
  def matchtype(types: Any) = types match {
    case a: String => println(s"this is string: $a")
    case b: Int => println(s"this is int: $b")
    case _ => println("can not match..")
  }

  //匹配Array
  def matcharray(arr: Any) = arr match {
    case Array(0) => println("only one data 0")
    case Array(0, _) => println("begin with 0, has two data")
    case Array(1, _, 3) => println("begin with 1 ,end with 3 ,has 3 data")
    case Array(8, _*) => println("begin with 8")
    case _ => println("can not match..")
  }

  //匹配List
  def matchlist(list: Any) = list match {
    case 0 :: Nil => println("only one data 0")
    case 7 :: 9 :: Nil => println("has two data 7 and 9")
    case x :: y :: z :: Nil => println("has three datas")
    case m :: n if n.length > 0 => println("has header and tail,tail not null")
    case m :: n => println("has header and tail")
    case _ => println("can not match..")
  }

  //匹配元组
  def matchtuple(tuple: Any) = tuple match {
    case (0, _) => println("tuple has two data,begin with 0")
    case (x, y, z) => println("tuple has three datas")
    case (_, "cat") => println("tuple has two data,end with cat")
    case _ => println("can not match..")
  }

  //匹配对象
  def matchobj(obj: Any) = obj match {
    case OneCaseObj(x) => println("hello OneCaseObj")
    case TwoCaseObj(x, y) => println("hello TwoCaseObj")
    case ThreeCaseObj => println("hello ThreeCaseObj")
    case _ => println("can not match..")
  }
}

case class OneCaseObj(a: String) {}

case class TwoCaseObj(a: String, b: String) {}

case class ThreeCaseObj() {}
