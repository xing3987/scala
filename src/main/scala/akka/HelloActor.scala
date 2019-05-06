package akka

import akka.actor.Actor

class HelloActor extends Actor {
  override def receive: Receive = {
    case "hello" => println("hello too..")
  }
}

object HelloActor {

}
