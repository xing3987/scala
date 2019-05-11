package demo

/**
  * 比较器Comparator,Comparable
  */
object CompareDemo20 {
  def main(args: Array[String]): Unit = {
    val book1 = new Book("bk1", 100)
    val book2 = new Book("bk2", 50)
    if (book1 > book2) println(book1) else println(book2)

    val box1 = new Box(10)
    val box2 = new Box(50)

    val list = List(box1, box2)

    //创建一个比较器，Ordering(对应java:Comparator)
    val compare = new Ordering[Box] {
      override def compare(x: Box, y: Box): Int = {
        x.size - y.size
      }
    }
    val boxes = list.sorted(compare)
    println(boxes)
  }
}


//一个类要可以比较要实现Ordered接口(对应java:Comparable)
class Book(val x: String, val money: Int) extends Ordered[Book] {
  override def compare(that: Book): Int = {
    this.money - that.money
  }

  override def toString = s"Book($x, $money)"
}

class Box(val size: Int){
  override def toString = s"Box($size)"
}
