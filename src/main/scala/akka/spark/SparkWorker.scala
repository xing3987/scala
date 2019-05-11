package akka.spark

import java.util.UUID

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._ // 导入时间单位millis

class SparkWorker(masterUrl: String) extends Actor {

  var selection: ActorSelection = _ //服务器的actorRef对象
  val workId = UUID.randomUUID().toString //创建workerId为uuid

  override def preStart(): Unit = {
    //连接服务器，指定当前client的名称,初始化服务器的actorRef.
    // 注意“robotserver”指定的名字要和服务器的名字一致，要不然消息会发送不到服务器
    selection = context.actorSelection(masterUrl)
  }

  override def receive: Receive = {
    //如果收到start的消息，则向服务器注册自己的信息
    case "start" => {
      println("work begin..")
      selection ! RegisterWorkerInfo(new WorkInfo(workId, "4", "1024*32"))
    }

    case RegisteredWorkerInfo => {
      //收到服务器发来的注册成功信息，启动定时给服务器发送心跳的任务
      import context.dispatcher // 使用调度器时候必须导入dispatcher
      context.system.scheduler.schedule(0 millis, 3000 millis, self, SendHeartBeat)
    }

    //给服务器发送心跳
    case SendHeartBeat => {
      // 开始向master发送心跳了
      println(s"------- $workId 发送心跳-------")
      selection ! HeartBeat(workId)
    }
  }
}

object SparkWorker {
  def main(args: Array[String]): Unit = {

    if (args.length < 4) {
      println(
        """
          |请输入参数：<host> <port> <WorkerName> <masterUrl>
        """.stripMargin)
      sys.exit()
    }

    val host = args(0)
    val port = args(1)
    val workerName = args(2)
    val masterUrl = args(3)

    //创建服务器端的配置，指定端口号和地址，两种格式都可以
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
    val sparkworker = factory.actorOf(Props(new SparkWorker(masterUrl)), workerName) //注意这里的名字robotserver用于客户端连接
    sparkworker ! "start"
  }
}
