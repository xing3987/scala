package akka.robot

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

/**
  * 机器人服务器端
  * server端启动后会暴露地址如：akka.tcp://robot@127.0.0.1:8888
  */
class RobotServer extends Actor {
  //用于接收客户端发送来的消息，做出自动回复
  override def receive: Receive = {
    case "start" => println("机器人准备，发射。。")
    case "你叫啥" => sender() ! "我叫小爱"
    case "你是男是女" => sender() ! "小爱是个美人"
    case "你有男朋友吗" => sender() ! "小爱还是个宝宝，嘿嘿"
    case _ => sender() ! "小爱听不懂你说的哦"
  }
}

object RobotServer {
  val host: String = "127.0.0.1"
  val port: Int = 8888

  def main(args: Array[String]): Unit = {
    //创建服务器端的配置，指定端口号和地址
    val config = ConfigFactory.parseString(
      /*      s"""
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
      """)

    val factory = ActorSystem("robot", config) //使用配置创建factory
    val robotserver = factory.actorOf(Props[RobotServer], "robotserver") //注意这里的名字robotserver用于客户端连接
    robotserver ! "start"
  }
}
