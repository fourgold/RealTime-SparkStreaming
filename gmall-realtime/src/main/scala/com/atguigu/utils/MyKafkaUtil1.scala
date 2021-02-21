package com.atguigu.utils

import java.util.Properties

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * @author Jinxin Li
 * @create 2020-12-02 14:25
 */
object MyKafkaUtil1 {
  private val properties: Properties = PropertiesUtil.load("config.properties")
  private val brokers: String = properties.getProperty("kafka.broker.list")
  private val group: String = properties.getProperty("kafka.group.id")

  val kafkaParams = Map(
    "bootstrap.servers" -> brokers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> group,
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )
  //kafka服务器列表
  //k与v的序列化
  //消费者组
  //如果没有初始化偏移量则会自动设置为latest
  //自动提交偏移量

  def getKafkaStream(ssc:StreamingContext, topic:String) ={
    KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(topic),kafkaParams))
  }
}
