package com.atguigu.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.constants.GmallConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;


@RestController
@Controller
@Slf4j
public class LoggerController{
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("testDemo")
    @ResponseBody//表示当前方法返回对象.平时应该是返回页面,但是现在没有页面,只有string
    public String test1(){
        System.out.println("11111111111111");
        return "success";
    }
    @RequestMapping("testDemo2")
    @ResponseBody//表示当前方法返回对象.平时应该是返回页面,但是现在没有页面,只有string
    public String test02(@RequestParam("name") String nn,
                         @RequestParam("age") int age) {//@RequestParam,请求参数,可以从web端接入参数,从网页中带入数据
        System.out.println(nn + ":" + age);
        return "success";
    }
    @RequestMapping("log")//产生映射,将带有log的请求拿过来
    @ResponseBody //表示返回值不是页面
    public String getLogger(@RequestParam("logString") String logString){
//        System.out.println(logString);
        //1.添加时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts", System.currentTimeMillis());

        //2.将jsonObject转换为字符串
        String logStr = jsonObject.toString();

        //System.out.println(logString);

        //3.将数据落盘
        log.info(logStr);

        //4.根据数据类型发送至不同的主题
        //type说明是一个启动日志
        if ("startup".equals(jsonObject.getString("type"))) {
            //启动日志
            kafkaTemplate.send(GmallConstant.GMALL_STARTUP, logStr);
        } else {
            //事件日志
            kafkaTemplate.send(GmallConstant.GMALL_EVENT, logStr);
        }
        //虚拟机查看kafka主题
        //bin/kafka-topics.sh --zookeeper hadoop102:2181/kafka --list
        //启动消费者
        //bin/kafka-console-consumer.sh --bootstrap-server hadoop102:9092 --topic TOPIC_START
        //bin/kafka-console-consumer.sh --bootstrap-server hadoop102:9092 --topic TOPIC_EVENT
        //bin/kafka-topics.sh --bootstrap-server hadoop102:9092 --create --replication-factor 1 --partitions 1 --topic TOPIC_START
        //bin/kafka-topics.sh --bootstrap-server hadoop102:9092 --create --replication-factor 1 --partitions 1 --topic TOPIC_EVENT

        return "success";
    }

}