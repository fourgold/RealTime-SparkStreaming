package com.atguigu.test

import com.atguigu.constants.GmallConstant
import com.atguigu.utils.{MyKafkaUtil1, PropertiesUtil1}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Jinxin Li
 * @create 2020-12-02 14:54
 */
object KafkaTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //使用常数类获取主题
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil1.getKafkaStream(ssc, GmallConstant.GMALL_EVENT)
    kafkaDStream.map(record=>record.value()).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
