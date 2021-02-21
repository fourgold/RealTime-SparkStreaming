package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.atguigu.constants.GmallConstant
import com.atguigu.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.mutable.ListBuffer

/**
 * @author Jinxin Li
 * @create 2020-12-07 15:20
 */
object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.消费Kafka订单以及订单明细主题数据创建流
    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_ORDER_INFO, ssc)
    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_ORDER_DETAIL, ssc)

    //4.将数据转换为样例类对象并转换结构为KV
    //OrderId的第一个流 join
    val orderIdToInfoDStream: DStream[(String, OrderInfo)] = orderInfoKafkaDStream.map(record => {
      //a.将value转换为样例类
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      //b.取出创建时间 yyyy-MM-dd HH:mm:ss
      val create_time: String = orderInfo.create_time
      //c.给时间重新赋值
      val dateTimeArr: Array[String] = create_time.split(" ")
      orderInfo.create_date = dateTimeArr(0)
      orderInfo.create_hour = dateTimeArr(1).split(":")(0)
      //d.数据脱敏
      val consignee_tel: String = orderInfo.consignee_tel
      val tuple: (String, String) = consignee_tel.splitAt(4)
      orderInfo.consignee_tel = tuple._1 + "*******"
      //e.返回结果
      (orderInfo.id, orderInfo)
    })

    //OrderId的第二个流 OrderId
    val orderIdToDetailDStream: DStream[(String, OrderDetail)] = orderDetailKafkaDStream.map(record => {
      //a.转换为样例类
      val detail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
      //b.返回数据
      (detail.order_id, detail)
    })

    //双流JOIN(普通JOIN)
    //    val value: DStream[(String, (OrderInfo, OrderDetail))] = orderIdToInfoDStream.join(orderIdToDetailDStream)
    //    value.print(100)

    //5.全外连接
    val fullJoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderIdToInfoDStream.fullOuterJoin(orderIdToDetailDStream)
    //注意全外连接是进来的一条数据
    //6.处理JOIN之后的数据
    val noUserSaleDetailDStream: DStream[SaleDetail] = fullJoinDStream.mapPartitions(iter => {

      //获取Redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //创建集合用于存放关联上的数据
      val details = new ListBuffer[SaleDetail]

      implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

      //遍历iter,做数据处理
      iter.foreach { case (orderId, (infoOpt, detailOpt)) =>

        //注意这两个key的设计,我吐了,原来orderInfo是字符串,<<<<<<<<<<<<<<<<<<<<<<<<这边key的设计是非常重要的
        //双流join的核心就在于相同的orderId应当存在一起,如果join上,写出到details
        /*
        步骤判断
          1.OrderInfo-OrderDetail(OrderInfo.OrderID=OrderDetail.OrderID)==>details
            1.1 OrderInfo==>redis(s"OrderInfo${OrderInfo.OrderID}" -> OrderInfo)
          2.OrderInfo-null==>redis(s"OrderInfo${OrderInfo.OrderID}" -> OrderInfo)
            2.1 OrderInfo==>redis(s"OrderDetail${OrderInfo.OrderID}" -> OrderDetail)
          3.null-OrderDetail==>redis(s"OrderInfo${OrderInfo.OrderID}" -> OrderDetail)
         */
        val infoRedisKey = s"OrderInfo:$orderId"
        val detailRedisKey = s"OrderDetail:$orderId"

        if (infoOpt.isDefined) {       
          //a.判断infoOpt不为空
          //取出infoOpt数据
          val orderInfo: OrderInfo = infoOpt.get

          //a.1 判断detailOpt不为空
          if (detailOpt.isDefined) {
            //取出detailOpt数据
            val orderDetail: OrderDetail = detailOpt.get
            //创建SaleDetail
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            //添加至集合
            details += saleDetail
          }

          //a.2 将info数据写入redis,给后续的detail数据使用
          // todo val infoStr: String = JSON.toJSONString(orderInfo)//编译通不过
          val infoStr: String = Serialization.write(orderInfo)
          jedisClient.set(infoRedisKey, infoStr)
          jedisClient.expire(infoRedisKey, 100)

          //a.3 找出在redis缓存中是否存在对应mid的订单细节
          if (jedisClient.exists(detailRedisKey)) {
            val detailJsonSet: java.util.Set[String] = jedisClient.smembers(detailRedisKey)

            //todo 获取对象sMembers之后asScala才能forEach
            detailJsonSet.asScala.foreach(detailJson => {
              //转换为样例类对象,并创建SaleDetail存入集合
              val detail: OrderDetail = JSON.parseObject(detailJson, classOf[OrderDetail])
              //创建集合用于存放关联上的数据
              details += new SaleDetail(orderInfo, detail)
            })
          }

        } else {
          //b.判断infoOpt为空
          //获取detailOpt数据
          val orderDetail: OrderDetail = detailOpt.get

          if (jedisClient.exists(infoRedisKey)) {
            //b.1 查询Redis中有OrderInfo数据
            //取出Redis中OrderInfo数据
            val infoJson: String = jedisClient.get(infoRedisKey)
            //转换数据为样例类对象
            val orderInfo: OrderInfo = JSON.parseObject(infoJson, classOf[OrderInfo])
            //创建SaleDetail存入集合
            details += new SaleDetail(orderInfo, orderDetail)
          } else {
            //b.2 查询Redis中没有OrderInfo数据
            // todo 使用serial
            val detailStr: String = Serialization.write(orderDetail)
            //写入Redis
            jedisClient.sadd(detailRedisKey, detailStr)
            jedisClient.expire(detailRedisKey, 100)
          }
        }

      }

      //归还连接
      jedisClient.close()

      //最终返回值
      details.iterator

    })

    //测试
//    saleDetailDStream.print(100)
    //测试
    //noUserSaleDetailDStream.print(100)

    //开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}
