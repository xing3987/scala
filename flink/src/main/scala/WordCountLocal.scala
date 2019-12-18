import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
object WordCountLocal {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text: DataSet[String] = env.fromElements("hello Tom", "hello lily", "nihao Tom", "I think I am haha")
    val result: AggregateDataSet[(String, Int)] = text.flatMap {_.toLowerCase.split(" ")}
      .map {(_, 1)}
      .groupBy(0)
      .sum(1)
    result.print()
  }
}
