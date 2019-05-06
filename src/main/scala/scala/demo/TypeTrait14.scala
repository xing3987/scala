package scala.demo

trait TypeTrait14 {
  type T

  def fly(x: T): Unit = {
    println("I am fly:" + x)
  }
}
