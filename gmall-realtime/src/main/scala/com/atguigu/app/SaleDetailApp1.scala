package com.atguigu.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.atguigu.constants.GmallConstant
import com.atguigu.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.{SPARK_BRANCH, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer
/**
 * @author Jinxin Li
 * @create 2020-12-07 15:20
 */
object SaleDetailApp1 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleDetailApp")
    //2.创建StreamingContext,设定间隔为5秒
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.1创建OrderDetail流 ConsumerRecord不可序列化,所以要想获取ConsumerRecord信息必须在executor端进行操作
    val kafkaOrderDetailDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_ORDER_DETAIL, ssc)
    //两个流的打印测试,注释掉,测试两个流是否能够从kafka中拿到数据
    /*kafkaOrderDetailDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
        iter.foreach(record=>{
          println("orderDetail"+record.value())
        })
      })
    })*/

    //3.2创建OrderInfo流
    val kafkaOrderInfoDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_ORDER_INFO, ssc)
    /*kafkaOrderInfoDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
        iter.foreach(record=>{
          println("orderInfo"+record.value())
        })
      })
    })*/

    //TODO 3 对orderInfo进行数据数据脱敏与增加时间
    val orderInfoTupleDStream: DStream[(String, OrderInfo)] = kafkaOrderInfoDStream.map(record => {
      val event: String = record.value()
      val orderInfoObject: OrderInfo = JSON.parseObject(event, classOf[OrderInfo])

      //对手机号脱敏
      val phoneTuple: (String, String) = orderInfoObject.consignee_tel.splitAt(3)
      val phoneHead: String = phoneTuple._1
      val phoneTail: String = phoneTuple._2.splitAt(4)._2
      orderInfoObject.consignee_tel = phoneHead + "****" + phoneTail

      //对时间重新赋值
      //日期
      orderInfoObject.create_date = orderInfoObject.create_time.split(" ")(0)
      //小时
      orderInfoObject.create_hour = orderInfoObject.create_time.split(" ")(1).split(":")(0)

      (orderInfoObject.id, orderInfoObject)
    })

    //TODO 4 对orderDetail进行转换成订单对象
    val orderDetailTupleDStream: DStream[(String, OrderDetail)] = kafkaOrderDetailDStream.map(
      record => {
        record.value()
        val orderDetailObject: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
        (orderDetailObject.order_id, orderDetailObject)
      }
    )

    //普通join
   /* val joinDStream: DStream[(String, (OrderInfo, OrderDetail))] = orderInfoTupleDStream.join(orderDetailTupleDStream)
    val saleDetailDStream = joinDStream.mapValues(t => {
      //调用saleDetail的构造方法
      val saleDetail = new SaleDetail(t._1, t._2)
      saleDetail
    })

    saleDetailDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
        iter.foreach(saleInfo=>{
          println(saleInfo._2)
        })
      })
    })*/

    // TODO 5 双流join
    val fullJoin: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoTupleDStream.fullOuterJoin(orderDetailTupleDStream)

    //已经获得了fullJoin的数据,要进行判断.并且根据判断进行落盘,这个时候选择mapPartition
    val SaleDetailDStream: DStream[SaleDetail] = fullJoin.mapPartitions(
      iter => {

        //分区内获得一个Redis客户端
        val jedisClient: Jedis = RedisUtil.getJedisClient

        //设置一个ListBuffer用于装detail
        val details = new ListBuffer[SaleDetail]

        iter.foreach {
          case (orderId, (orderInfoObt, orderDetailObt)) =>

            // todo a判断 是否存在OrderInfo对象 左值
            if (orderInfoObt isDefined) {
              val orderInfo: OrderInfo = orderInfoObt.get //将option格式转换成样例类

              // todo a.1 关联成功
              if (orderDetailObt.isDefined) {
                val saleDetail = new SaleDetail(orderInfo, orderDetailObt.get)
                details += saleDetail
              }

              // todo a.2 写入缓存 key UserInfo:OrderId V UserInfo
              jedisClient.set(s"OrderInfo$orderId", orderInfo.toString)

              //设定失效时间
              jedisClient.expire(s"OrderInfo$orderId", 1000)

              //todo a.3 查询缓存中是否存在对应的OrderDetail OrderDetail用set存储
              if (jedisClient.exists(s"OrderDetail$orderId")) {
                val OrderDetailStrings: util.Set[String] = jedisClient.smembers(s"OrderDetail$orderId")
                OrderDetailStrings.forEach(str => {
                  val orderDetail: OrderDetail = JSON.parseObject(str, classOf[OrderDetail])
                  val saleDetail = new SaleDetail(orderInfo, orderDetail)
                  details += saleDetail
                })
              }
            }
            // todo b 判断 是否存在右值 orderDetail
            else if (orderDetailObt isDefined) {

              //todo 查询缓存中是否存在对应的orderInfo
              if (jedisClient.exists(s"OrderInfo$orderId")) {
                val orderInfo: String = jedisClient.get(s"OrderInfo$orderId")
                val orderInfoObject: OrderInfo = JSON.parseObject(orderInfo, classOf[OrderInfo])
                //merge
                val saleDetail = new SaleDetail(orderInfoObject, orderDetailObt.get)
                //将数据存入数组
                details += saleDetail
              } else {

                //todo 如果没有join上,说明此orderDetail不在现在跟以前,只能在将来,所以把orderDetail放置在redis
                jedisClient.sadd(s"OrderDetail$orderId", orderDetailObt.get.toString)
                jedisClient.expire(s"OrderDetail$orderId", 1000)
              }

            }
        }

        //todo 归还连接
        jedisClient.close()

        details.toIterator
      })


    //打印
    SaleDetailDStream.foreachRDD(rdd=>{
      rdd.foreach(info=>{
        println(info.toString)
      })
    })


    //开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}
