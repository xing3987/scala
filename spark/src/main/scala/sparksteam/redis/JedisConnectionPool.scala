package sparksteam.redis

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * 创建redis连接池
  */
object JedisConnectionPool {

  val config = new JedisPoolConfig
  //设置最大连接数
  config.setMaxTotal(20)
  //设置最大空闲连接数(初始连接数)
  config.setMaxIdle(10)
  //当调用borrow Object方法时，是否进行有效性检查
  config.setTestOnBorrow(true)
  //建立连接池：10000代表超时时间（10秒）
  private val pool = new JedisPool(config, "hadoop003", 6379, 10000, "root")

  //获取一个redis连接
  def getConnection(): Jedis = {
    pool.getResource
  }

  def main(args: Array[String]): Unit = {
    val conn = JedisConnectionPool.getConnection()

    var keys = conn.keys("*")
    import scala.collection.JavaConversions._ //操作java的集合需要导入隐式转换
    for (key <- keys) {
      println(key + ":" + conn.get(key))
    }
  }
}
