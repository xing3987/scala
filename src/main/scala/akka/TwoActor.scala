package akka

import akka.actor.{Actor, ActorRef, ActorSystem, Props}

/**
  * 模拟两人通信的actor,把一个对象的actorRef传递给另一个对象，达到交互通信的目的
  */
class TwoActor extends Actor {


  override def receive: Receive = {
    case "start" => println("准备开始。。")
    case "你好呀" => {
      Thread.sleep(1000)
      println(this.getClass + "你也好")
      sender() ! "你也好"
    }
    case "大家好才是真的好" => {
      println(this.getClass + "我们都是好盆友")
      sender() ! "我们都是好盆友"
    }
    case _ => {
      println(this.getClass + "说什么呢")
      sender() ! "你也好"
    }
  }


}

class OneActor(ref: ActorRef) extends Actor {

  override def receive: Receive = {
    case "你也好" => {
      Thread.sleep(1000)
      println(this.getClass + "大家好才是真的好")
      ref ! "大家好才是真的好"
    }
    case "我们都是好盆友" => {
      println(this.getClass + "你好呀")
      ref ! "你好呀"
    }
    case _ => {
      println(this.getClass + "说什么呢")
      ref ! "你好呀"
    }
  }
}

object TwoActor {
  val factory = ActorSystem("connection")
  val actorRef = factory.actorOf(Props[TwoActor], "two") //创建TwoActor的ref对象
  val actorRefone = factory.actorOf(Props(new OneActor(actorRef)), "one") //创建OneActor的ref对象

  def main(args: Array[String]): Unit = {
    actorRef ! "start"
    actorRefone ! "你也好"
  }
}
