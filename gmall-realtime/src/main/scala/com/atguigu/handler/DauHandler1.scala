package com.atguigu.handler

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.{PropertiesUtil1, RedisUtil, RedisUtil1}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.phoenix.spark.toProductRDDFunctions
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis


/**
 * @author Jinxin Li
 * @create 2020-12-02 18:42
 */
object DauHandler1 {
  private val properties: Properties = PropertiesUtil1.load("config.properties")
  private val table: String = properties.getProperty("table.name")
  private val zkUrl: String = properties.getProperty("zkUrl")
  /**
   * 将数据保存在hbase
   * @param filterdByMid
   */
  def saveMid2Phoenix(filterdByMid: DStream[StartUpLog]) = {
    filterdByMid.foreachRDD{_.saveToPhoenix(table,Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),HBaseConfiguration.create(),Some(zkUrl))}
  }

  private val sdf = new SimpleDateFormat("yyyy-MM-dd")
  /**
   * 同批次去重操作
   * 处理之前要先声明我们需要的是什么? DStream[StartUpLog]
   * 我们拿到的是什么? 将里面的样例类去重 group_by
   */
  def filterByMid(redisLogDStream: DStream[StartUpLog]): DStream[StartUpLog] = {
    val value = redisLogDStream
      .map(startLog => ((startLog.mid,startLog.logDate), startLog))
      .groupByKey()
    value.map{case (info,log)=>log.toList.sortWith(_.ts<_.ts).head}
  }

  /**
   * 将登陆过的用户写入redis
   *
   * @param startLogDStream
   */
  def saveMidToRedis(startLogDStream: DStream[StartUpLog]) = {
    startLogDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
        val client: Jedis = RedisUtil1.getJedisClient
        iter.foreach(log=>{
          client.sadd(s"DAU:${log.logDate}",s"DAU:${log.mid}")
        })
        client.close()
      })
    })


  }

  /**
   * 跨批次去重
   *
   * @param startLogDStream
   * @return
   */
  def filterByRedis(startLogDStream: DStream[StartUpLog],sc:SparkContext*)= {

    /*//解决1 跨批次去重,单条过滤,频繁拿取连接,拖慢效率
    startLogDStream.filter{
      log=>{
        val jedisClient: Jedis = RedisUtil1.getJedisClient
        val boolean: lang.Boolean = jedisClient.sismember(s"DAU:${log.logDate}", s"DAU:${log.mid}")
        jedisClient.close()
        !boolean
      }
    }*/
    //解决2 使用rdd分区操作,executor内分区执行
    /*startLogDStream.transform(_.mapPartitions{
      iter=> {
        println("------------------------------")
        println(Thread.currentThread().getName)
        println("------------------------------")
        val jedisClient: Jedis = RedisUtil1.getJedisClient
        val logs: Iterator[StartUpLog] = iter.filter {
          log => {
            val boolean: lang.Boolean = jedisClient.sismember(s"DAU:${log.logDate}", s"DAU:${log.mid}")
            !boolean
          }
        }
        jedisClient.close()
        logs
      }
    })*/

    //解决3 使用rdd分区操作,executor分区操作固然不错,但是中间还是每批次没分区都要对redis连接池进行访问
    //使用广播变量的方式
    startLogDStream.transform{
      rdd=>{
        val jedisClient: Jedis = RedisUtil1.getJedisClient//获取jedis连接池
        println("---------------广播变量获取连接池------------")
        println(Thread.currentThread().getName)
        println("---------------广播变量获取连接池------------")
        val set: util.Set[String] = jedisClient.smembers(s"DAU:${sdf.format(new Date(System.currentTimeMillis()))}")
        jedisClient.close()
        val midSetBC: Broadcast[util.Set[String]] = sc.head.broadcast(set)
        rdd.filter(log=>{!midSetBC.value.contains(log.mid)})
      }
    }

  }
}
