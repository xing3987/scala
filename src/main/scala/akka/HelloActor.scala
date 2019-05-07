package akka

import akka.actor.{Actor, ActorSystem, Props}

class HelloActor extends Actor {

  //接收消息后的反应
  override def receive: Receive = {

    case "hello" => println("hello too..")
    case "你好" => println("你好!")
    case "stop" => {
      context.stop(self) //停止receive
      context.system.terminate()  //停止工程运行
    }

  }
}

object HelloActor {

  val factory = ActorSystem("factory")
  val actorRef = factory.actorOf(Props[HelloActor])

  def main(args: Array[String]): Unit = {
    actorRef ! "hello"
    actorRef ! "你好"
    actorRef ! "stop"
  }
}
