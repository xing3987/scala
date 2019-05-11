package demo

import java.io.{BufferedReader, File, FileReader}


/**
  * 思考： 我们调用别人的框架，发现少了一些方法，需要添加，但是让别人为你一个人添加
  * 是不可能滴,可以使用implicit扩展
  * 隐式参数必须放在最后
  * 不能在同个类中定义多个相同类型的隐式参数，会报冲突（不能有歧义）
  * 同名的隐式函数function和隐式方法，优先function
  */
object ImplicitDemo17 {
  def main(args: Array[String]): Unit = {
    val x: Int = 3.14 //从上下文中找到double转换成int的方法，会自动调用(下划线标注)
    implicit val msg: String = "你好" //定义一个隐式变量
    //say("你好")
    say //编译器自动寻找隐式String参数msg

    implicit val a: Int = 6
    println(add(5)) //5+6
    println(add3(5)) //5+6+6

    say(33)

    Helper.printlnSplit()
    //使用implicit对类进行扩展(隐式类的转换)
    val file = new File("G:\\logs\\logdatas\\sample.log")
    println("count:" + file.count()) //默认调用file2RichFile把File转换成RichFile，调用RichFile的方法count()
  }

  //定义一个隐式方法，把double转成Int
  /*    implicit def double2Int(a: Double): Int = {
        a.toInt
      }*/

  //def say(content: String) = println(content)

  //定义隐式参数的方法
  def say(implicit content: String = "hello") = println(content)

  //隐式参数和柯拟化,隐式参数必须放在最后
  def add(a: Int)(implicit b: Int) = a + b

  //多个隐式参数
  def add3(a: Int)(implicit b: Int, c: Int) = a + b + c

  //定义一个隐式的方法，需要返回参数
  implicit val double2Int: (Double) => Int = (a: Double) => a.toInt
  //val double2Int = (dou: Double) => dou.toInt

  implicit val Int2String: (Int) => String = (a) => s"arg=> $a:" + a.toString


  //定义一个隐式方法，默认把File转成RichFile,可以把隐式方法提取到一个类中，导入使用
  implicit def file2RichFile(file: File): RichFile = {
    new RichFile(file)
  }
}


class RichFile(file: File) {
  //定义一个方法，返回文件的行数
  def count(): Int = {
    var sum = 0
    var fileReader = new FileReader(file)
    val bufferedReader = new BufferedReader(fileReader)
    try {
      var line = bufferedReader.readLine()
      while (line != null) {
        sum += 1
        line = bufferedReader.readLine()
      }
    } catch {
      case _: Exception => sum
    } finally {
      fileReader.close()
      bufferedReader.close()
    }
    sum
  }
}
