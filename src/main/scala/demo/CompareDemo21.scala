package demo

/**
  * 上界(Upper Bounds)/下界(lower bounds)
  * 注意隐式转换的方法定义在main方法上面，要不然可能发生运行错误
  */
object CompareDemo21 {
  //定义一个隐式转换的方法，让Boxs实现Ordered接口
  implicit def boxs2Orderd(b: Boxs) = new Ordered[Boxs] {
    override def compare(that: Boxs): Int = b.size - that.size
  }

  //定义一个隐式转换的方法，让Boxs实现Ordered接口
/*  implicit def boxs2Ordering(b: Boxs) = new Ordering[Boxs] {
    override def compare(that: Boxs): Int = {
      b.size - that.size
    }
  }*/

  def main(args: Array[String]): Unit = {
    val a = 1
    val b = 2
    //val c = new Cmp[Integer](a, b) //Int类型没有实现comparable接口(Predef中有Int转Integer方法)
    val c = new Cmp1(a, b)
    println(c.bigger)

    val box1 = new Boxs(10)
    val box2 = new Boxs(50)

    println(new Cmp(box1, box2).bigger)

    //println(new Cmp2(box1, box2).bigger)
  }
}


/**
  * 表示这个类的参数，必须实现Comparable.
  * "<:" 表示上界
  * "<%" 视图界定会自动发生隐式转换
  * " : " 直接使用上下文界限，效果同"<%"
  */
//class Cmp[T <: Comparable[T]](o1: T, o2: T) {
class Cmp1[T <% Comparable[T]](o1: T, o2: T) {

  //因为o1,o2都实现了comparable接口所以他们是可以比较的
  def bigger = if (o1.compareTo(o2) > 0) o1 else o2
}


//使用scala比较器ordered
class Cmp[T <% Ordered[T]](o1: T, o2: T) {

  //因为o1,o2都实现了comparable接口所以他们是可以比较的
  def bigger = if (o1 > o2) o1 else o2
}

//使用scala比较器Ordering
/*class Cmp2[T : Ordering[T]](o1: T, o2: T) {

  //因为o1,o2都实现了comparable接口所以他们是可以比较的
  def bigger = {
    val cmptor=implicitly[Ordering[T]]  //从上下文中获得比较器
    if(cmptor.compare(o1,o2)>0) o1 else o2
  }
}*/

class Boxs(val size: Int) {
  override def toString = s"Box($size)"
}

