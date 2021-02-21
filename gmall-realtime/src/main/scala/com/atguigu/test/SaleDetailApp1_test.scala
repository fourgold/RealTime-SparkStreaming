package com.atguigu.test

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.atguigu.constants.GmallConstant
import com.atguigu.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.streaming.CommitMetadata.format
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.mutable.ListBuffer

/**
 * @author Jinxin Li
 * @create 2020-12-10 23:35
 * todo 关于往redis写之前进行serialization.write()操作序列化 还有redis中,提取出set之后asScala转换问题
 */
object SaleDetailApp1_test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TestApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //todo 1消费Kafka订单明细
    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_ORDER_DETAIL, ssc)

    orderDetailKafkaDStream.foreachRDD(iter=>{


      iter.map(record=>{

        //todo 1.1获取redis连接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        val orderDetailString: String = record.value()

        // todo 1.2转换为样例类
        val orderDetailObject: OrderDetail = JSON.parseObject(orderDetailString, classOf[OrderDetail])

        // todo 1.3将orderDetail数据写入redis toString  toJSONString Serialization

        //todo --------------------------------------------------
        jedisClient.sadd(s"toString:${orderDetailObject.id}",orderDetailObject.toString)

        if (jedisClient.exists(s"toString:${orderDetailObject.id}")) {
          val detailJsonSet: java.util.Set[String] = jedisClient.smembers(s"toString:${orderDetailObject.id}")

          //todo 获取对象sMembers之后asScala才能forEach
          detailJsonSet.asScala.foreach(detailJson => {
            println("使用asScala打印得到的orderDetail"+detailJson)
          })
        }

        /*//todo --------------------------------------------------
        val json: String = JSON.toJSONString(orderDetailObject)
        jedisClient.sadd(s"json:${orderDetailObject.id}",json)

        if (jedisClient.exists(s"json:${orderDetailObject.id}")) {
          val detailJsonSet: java.util.Set[String] = jedisClient.smembers(s"json:${orderDetailObject.id}")

          //todo 获取对象sMembers之后asScala才能forEach
          detailJsonSet.asScala.foreach(detailJson => {
            println("使用asScala打印得到的orderDetail"+detailJson)
          })
        }*/

        //todo --------------------------------------------------
        val serialString: String = Serialization.write(orderDetailObject)
        jedisClient.sadd(s"serial:${orderDetailObject.id}",serialString)

        if (jedisClient.exists(s"serial:${orderDetailObject.id}")) {
          val detailJsonSet: java.util.Set[String] = jedisClient.smembers(s"serial:${orderDetailObject.id}")

          //todo 获取对象sMembers之后asScala才能forEach
          detailJsonSet.asScala.foreach(detailJson => {
            println("使用asScala打印得到的orderDetail"+detailJson)
          })
        }

        //关闭客户端
        jedisClient.close()
      })

    })


         /* val infoStr: String = Serialization.write(orderInfo)

            val detailJsonSet: java.util.Set[String] = jedisClient.smembers(detailRedisKey)
            detailJsonSet.asScala*/


    //开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}
