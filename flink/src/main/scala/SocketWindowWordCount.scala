import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object SocketWindowWordCount {
  def main(args: Array[String]): Unit = {
    //定义一个枚举类，保持单词和计数
    case class WordAndCount(word: String, count: Int)
    //port表示需要链接的端口
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port");
    } catch {
      case e: Exception => {
        println(e.getMessage)
        return
      }
    }
    //获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val text: DataStream[String] = env.socketTextStream("localhost", port, ' ')
    //加上隐式转换，使用rdd
    import org.apache.flink.api.scala._
    val result:DataStream[WordAndCount] = text.flatMap {
      _.split(" ")}
      .map {WordAndCount(_, 1)} //数据转成对象
      .keyBy("word") //聚合函数
      .timeWindow(Time.seconds(5)) //设置收集处理流的时间间隔
      .sum("count") //求和

    //打印输出并设置使用一个并行度
    result.print().setParallelism(1)
    env.execute("socket Window WordCount")
}
}
