package akka.spark

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import scala.collection.mutable._
import scala.concurrent.duration._ // 导入时间单位millis

class SparkMaster extends Actor {

  //储存work信息的hashmap,key->id,value->WorkInfo
  var maps = HashMap[String, WorkInfo]()

  override def receive: Receive = {
    case start => println("spark master start...")

    case RegisterWorkerInfo(workinfo) => {
      maps += ((workinfo.id, workinfo)) //把传递来的workinfo信息保存到maps中
      // master存储完worker注册的数据之后，要告诉worker说你已经注册成功
      sender() ! RegisteredWorkerInfo // 此时worker会收到注册成功消息
    }
    // worker给master发送心跳信息
    case HearBeat(id) => {
      //遍历maps，更新worker的最后一次心跳信息
      maps(id).lastHeartBeatTime = System.currentTimeMillis()
    }

    //收到确认worker的消息，启动schedule定时检查worker的生存状态，删除失效worker
    case CheckTimeOutWorker => {
      //import context.dispatcher // 使用调度器时候必须导入dispatcher
      //定义一个调度器每6秒执行一次，移除死亡的worker
      context.system.scheduler.schedule(0 millis, 6000 millis, self, RemoveTimeOutWorker)
    }

    //删除超时worker
    case RemoveTimeOutWorker => {
      // 将hashMap中的所有的value都拿出来，查看当前时间和上一次心跳时间的差 3000
      val workerInfos = maps.values
      val currentTime = System.currentTimeMillis()
      workerInfos.filter(wkinfo => (currentTime - wkinfo.lastHeartBeatTime) >= 3000) //移除3秒未响应的worker
        .foreach(wkinfo => maps.remove(wkinfo.id))

      println(s"-----还剩 ${maps.size} 存活的Worker-----")
    }
  }
}

object SparkMaster {
  def main(args: Array[String]): Unit = {

    if (args.length <= 3) {
      println(
        """
          |请输入参数：<host> <port> <masterName>
        """.stripMargin)
      sys.exit()
    }

    val host = args(0)
    val port = args(1)
    val masterName = args(2)

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
    val sparkmaster = factory.actorOf(Props[SparkMaster], masterName) //注意这里的名字robotserver用于客户端连接
    sparkmaster ! "start"
    sparkmaster ! CheckTimeOutWorker
  }
}
