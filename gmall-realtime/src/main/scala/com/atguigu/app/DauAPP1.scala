package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstant
import com.atguigu.handler.{DauHandler, DauHandler1}
import com.atguigu.utils.{MyKafkaUtil, MyKafkaUtil1, RedisUtil1}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Jinxin Li
 * @create 2020-12-02 14:19
 * 业务需求,查看每日日活(需要去重)
 */
object DauAPP1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val kafkaDStream = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_STARTUP,ssc)

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH")

    val startLogDStream: DStream[StartUpLog] = kafkaDStream
      .map(record => record.value())
      .map(logs => {
        val log: StartUpLog = JSON.parseObject(logs, classOf[StartUpLog])
        val logDate = dateFormat.format(new Date(log.ts)).split(" ")
        log.logDate=logDate(0)
        log.logHour=logDate(1)
        log
      })
    startLogDStream.cache()
    startLogDStream.count().print()
    println("---------------------------------------")
    //1.利用redis跨批次去重
    val redisLogDStream: DStream[StartUpLog] = DauHandler1.filterByRedis(startLogDStream,ssc.sparkContext)

    //2.利用spark算子,同批次去重
    val filterdByMid: DStream[StartUpLog] = DauHandler1.filterByMid(redisLogDStream)

    //3.将去重之后的数据中的Mid保存到Redis(为了后续批次去重)
    DauHandler1.saveMidToRedis(filterdByMid)

    //4.将去重之后的数据明细保存在Phoenix
    DauHandler1.saveMid2Phoenix(filterdByMid)
    ssc.start()
    ssc.awaitTermination()
  }
}
