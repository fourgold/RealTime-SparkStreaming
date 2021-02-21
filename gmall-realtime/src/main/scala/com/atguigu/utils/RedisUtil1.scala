package com.atguigu.utils

import java.util.Properties

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * @author Jinxin Li
 * @create 2020-12-02 18:32
 */
object RedisUtil1 {

  var jedisPool:JedisPool = _

  def getJedisClient ={
    if (jedisPool == null) {
      println("开辟一个redis连接池")
      val properties: Properties = PropertiesUtil1.load("config.properties")
      val redisHost: String = properties.getProperty("redis.host")
      val redisPort: String = properties.getProperty("redis.port")

      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100)
      jedisPoolConfig.setMaxIdle(20)
      jedisPoolConfig.setMinIdle(20)
      jedisPoolConfig.setBlockWhenExhausted(true)
      jedisPoolConfig.setMaxWaitMillis(500)
      jedisPoolConfig.setTestOnBorrow(true)

      jedisPool=new JedisPool(jedisPoolConfig,redisHost,redisPort.toInt)
    }
//    println(s"jedisPool.getNumActive = ${jedisPool.getNumActive}")
    jedisPool.getResource
  }

  def main(args: Array[String]): Unit = {
    val client: Jedis = RedisUtil1.getJedisClient
    println(s"jedisPool.getNumActive = ${client}")
    client.close()
  }
}
