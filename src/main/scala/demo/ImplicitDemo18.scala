package demo

import java.io.File

import scala.io.Source

/**
  * 需求给File对象添加一个read方法，返回文件类容
  * 隐式类可以在静态类(object)中创建
  */
object ImplicitDemo18 {
  def main(args: Array[String]): Unit = {
    val file = new File("G:\\logs\\logdatas\\sample.log")
    println(file.read)
  }

  //创建隐式类
  implicit class RichsFile(file: File) {
    def read = Source.fromFile(file).mkString
  }

}
