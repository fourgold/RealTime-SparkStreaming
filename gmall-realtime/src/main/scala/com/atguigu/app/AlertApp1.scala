package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstant
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks.{break, breakable}

/**
 * @author Jinxin Li
 * @create 2020-12-07 9:14
 */

/**
 * >>>>>明确产生预警的需求
 * 1.同一设备 5分钟内三次以上用不同账号登录并领取优惠券
 * 2.并且过程中没有浏览商品,也就是说没有浏览商品的行为,存在浏览商品的行为则不发预警(设置标志位)
 * 并且同一设备,每分钟只记录一次预警(五分钟的窗口)(使用幂等性检测ES幂等性.DOC_ID)
 *
 * 分组 groupByKey
 * window本质是union
 */
object AlertApp1 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")
    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.消费Kafka TOPIC_EVENT主题数据创建流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_EVENT, ssc)

    //4.将每行数据转换为样例类,补充时间字段,并将数据转换为KV结构(mid,log)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

    val midToLogDStream: DStream[(String, EventLog)] = kafkaDStream.map(record => {

      //a.转换为样例类,将从kafka消费过来的数据进行转换为EventLog样例类
      val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])

      //b.处理时间
      val dateHourStr: String = sdf.format(new Date(eventLog.ts))
      val dateHourArr: Array[String] = dateHourStr.split(" ")
      eventLog.logDate = dateHourArr(0)
      eventLog.logHour = dateHourArr(1)

      //c.返回数据
      (eventLog.mid, eventLog)
    })

    //5.开窗5min,记录 五分钟内所有的动作日志
    val midToLogByWindowDStream: DStream[(String, EventLog)] = midToLogDStream.window(Minutes(5))

    //6.按照mid分组,(eventLog.mid, eventLog),对数据在五分钟之内按照mid进行分组,去重
    val midLog2MidLogGroupDStream: DStream[(String, Iterable[EventLog])] = midToLogByWindowDStream.groupByKey()

    //7.组内筛选数据,现在的数据类型为(mid,iterable(eventLog1,eventLog2...),input数据为进入一个mid的所有动作日志
    val AlertInfo= midLog2MidLogGroupDStream.map { case (mid, iter) => {
      //创建Set用于存放领券的uid
      val uidSet = new util.HashSet[String]()

      //创建Set用于存放优惠券涉及的商品ID
      val itemIdSet = new util.HashSet[String]()

      //创建List用于存放发生过的所有行为,将每条日志的行为存储进去,不需要去重
      val eventArray = new util.ArrayList[String]()

      //定义标志位,用于记录是否存在浏览商品行为
      var flag: Boolean = true

      //遍历iter
      breakable {
        iter.foreach(
          log => {
            eventArray.add(log.evid)
            if ("clickItem".equals(log.evid)) {
              flag = false
              break()
            } else if ("coupon".equals(log.evid)) {
              uidSet.add(log.uid)
              itemIdSet.add(log.itemid)
            }
          }
        )
      }
      (flag && uidSet.size() > 3, CouponAlertInfo(mid, uidSet, itemIdSet, eventArray, System.currentTimeMillis()))
    }
    }
    //8.将数据筛选出来
    val AlertInfoDStream: DStream[CouponAlertInfo] = AlertInfo.filter(_._1).map(_._2)


    //测试产生预警信息
    AlertInfoDStream.print(100)
    //9.写入ES,将预警信息写入
    AlertInfoDStream.foreachRDD(
      rdd=> {
        rdd.foreachPartition {
              //对每个分区的迭代器
          iter=>{
            //创建索引名
            val todayStr: String = sdf.format(new Date(System.currentTimeMillis())).split(" ")(0)
            //索引名的格式为 gmall_coupon_alert-todayStr
            val indexName = s"${GmallConstant.ES_ALERT_INDEX_PRE}-$todayStr"
            val iterator: Iterator[(String, CouponAlertInfo)] = iter.map(alterInfo => {
              val min: Long = alterInfo.ts / 1000 / 60 //计算距离当今的时间
              (s"${alterInfo.mid}-${min}", alterInfo)
            })
            MyEsUtil.insertBulk(indexName,iterator.toList)
          }
        }
      }
    )
    //10.启动任务
    ssc.start()
    ssc.awaitTermination()

  }
}
