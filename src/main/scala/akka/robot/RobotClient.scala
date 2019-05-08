package akka.robot

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.io.StdIn

/**
  * 机器人客户端，用于和机器人交互通信
  */
class RobotClient extends Actor {

  var selection: ActorSelection = _ //服务器的actorRef对象

  //重写初始化方法
  override protected[akka] def aroundPreStart(): Unit = {
    //连接服务器，指定当前client的名称,初始化服务器的actorRef.
    // 注意“robotserver”指定的名字要和服务器的名字一致，要不然消息会发送不到服务器
    selection = context.actorSelection("akka.tcp://robot@127.0.0.1:8888/user/robotserver")
  }

  override def receive: Receive = {
    case "start" => println("client begin..")
    case ServerMessage(msg) => {
      //输出所有收到的信息
      println("server:" + msg)
    }
    case ClientMessage(msg) => {
      selection ! ClientMessage(msg)
    }
  }
}

object RobotClient {
  val host: String = "127.0.0.1"
  val port: Int = 9998 //端口不能和服务器端冲突

  def main(args: Array[String]): Unit = {
    //创建服务器端的配置，指定端口号和地址
    val config = ConfigFactory.parseString(
      /*s"""
        |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
        |akka.remote.netty.tcp.hostname=$host
        |akka.remote.netty.tcp.port=$port
      """.stripMargin*/
      s"""
        akka {
          actor {
            provider = "akka.remote.RemoteActorRefProvider"
          }
          remote {
            netty.tcp {
              hostname = $host
              port = $port
            }
          }
        }
      """
    )

    val factory = ActorSystem("client", config) //使用配置创建factory
    val robotclient = factory.actorOf(Props[RobotClient], "robotclient")
    robotclient ! "start"
    //客户端发送消息，从console中得到
    while (true) {
      val str = StdIn.readLine() //按行读取用户在console的输入内容
      robotclient ! ClientMessage(str)
    }
  }
}