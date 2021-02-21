package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.UserInfo
import com.atguigu.constants.GmallConstant
import com.atguigu.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * @author Jinxin Li
 * @create 2020-12-08 11:05
 * 将用户表新增及变化数据缓存至Redis
 */

object UserInfoApp1 {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UserInfoAPP")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.消费Kafka用户主题数据创建流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_USER_INFO, ssc)

    kafkaDStream.foreachRDD(rdd=>rdd.foreach(record=>{
      val userInfo: String = record.value()
      println(userInfo)
    }))

    //4.取出Value
    val userInfoDStream: DStream[String] = kafkaDStream.map(_.value())

    //5.将用户数据写入Redis
    userInfoDStream.foreachRDD(rdd=>{

      rdd.foreachPartition {

        println("iter外部"+Thread.currentThread().getName)//在Partition端是Driver

        //a.获取连接
        iter => {
          //内部才是executor,保证连接在executor内部开辟
          println("iter内部"+Thread.currentThread().getName)
          val jedisClient: Jedis = RedisUtil.getJedisClient
          iter.foreach(
            userJson=>{
            val userInfo: UserInfo = JSON.parseObject(userJson, classOf[UserInfo])
              //设计redis的key,redis的key首先设定redis的标志位
            jedisClient.set(s"UserInfo:${userInfo.id}",userJson)//这个地方要解析出UserInfo
          })
          jedisClient.close()
        }
      }
    })

    //开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}
