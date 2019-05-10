package akka.spark

/**
  * worker -> master
  */
// worker向master注册自己（信息）
case class RegisterWorkerInfo(workinfo: WorkInfo)
// worker给master发送心跳信息
case class HearBeat(id: String)


/**
  * master -> worker
  */
// master向worker发送注册成功消息
case object RegisteredWorkerInfo



//master自己给自己发送一个检查超时worker的信息,并启动一个调度器，周期新检测删除超时worker
case object CheckTimeOutWorker

// master发送给自己的消息，删除超时的worker
case object RemoveTimeOutWorker

//定义work对象的属性，id,核数，内存，用于sparkmaster分配工作
class WorkInfo(val id: String, core: String, ram: String) {
  var lastHeartBeatTime: Long = System.currentTimeMillis() //定义一个最后心跳的时间变量
}