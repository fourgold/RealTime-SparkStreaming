package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.constants.GmallConstant
import com.atguigu.utils.{MyKafkaUtil, PropertiesUtil1}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.phoenix.spark.toProductRDDFunctions
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Jinxin Li
 * @create 2020-12-04 9:56
 */
object GmvApp1 {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    //注意生产环境下不能setMatser
    //首先总核数等于分区数,direct
    //--master --executor --core???
    val conf: SparkConf = new SparkConf().setAppName("GmvApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    //3.消费Kafka订单主题数据创建流
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_ORDER_INFO, ssc)

    //4.将每行数据转换为样例类对象,补充时间,数据脱敏(手机号)
    val orderInfoDStream: DStream[OrderInfo] = kafkaStream.map(record => {
      val value: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])//订单时间修改
      orderInfo.create_date = orderInfo.create_time.split(" ")(0)
      orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
      val consignee_tel: String = orderInfo.consignee_tel//手机号脱敏
      val tuple: (String, String) = consignee_tel.splitAt(4)
      orderInfo.consignee_tel = tuple._1 + "*******"
      orderInfo
    })

    //Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR")
    //5.写入Phoenix
    orderInfoDStream.foreachRDD(rdd=>rdd.saveToPhoenix(
      "GMALL200720_ORDER_INFO",
      classOf[OrderInfo].getDeclaredFields.map(_.getName.toUpperCase),
      HBaseConfiguration.create(),
      Some(PropertiesUtil1.load("config.properties").getProperty("zkUrl"))
    ))

    //6.开启任务
    ssc.start()
    ssc.awaitTermination()

  }
}
