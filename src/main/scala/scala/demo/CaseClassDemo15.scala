package scala.demo

/**
  * 样例类case class支持模式匹配，默认实现了serializable接口
  */
case class CaseClassDemo15(sender: String, message: String) {
}

/**
  * 样例对象,也是用于模式匹配
  * 样例对象不能封装数据
  */
case object CaseObject{

}