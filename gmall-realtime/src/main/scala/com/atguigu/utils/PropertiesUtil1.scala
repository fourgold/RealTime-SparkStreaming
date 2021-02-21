package com.atguigu.utils

import java.io.InputStreamReader
import java.util.Properties


/**
 * @author Jinxin Li
 * @create 2020-12-02 14:27
 */
object PropertiesUtil1 {
  def load(PropertiesName:String) ={
    //首先创建配置文件
    val properties = new Properties()
    //因为配置文件的导入需要书写大量的类,所以这边我们需要把这个进行包装
    properties.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(PropertiesName),"UTF-8"))
    properties
  }

  def main(args: Array[String]): Unit = {
    //测试一下能不能正确获得配置
    println(PropertiesUtil.load("config.properties").getProperty("redis.port"))
  }
}
