package com.atguigu.bean

/**
 * @author Jinxin Li
 * @create 2020-12-01 23:39
 */
case class StartUpLog(mid: String,
                      uid: String,
                      appid: String,
                      area: String,
                      os: String,
                      ch: String,
                      `type`: String,
                      vs: String,
                      var logDate: String,
                      var logHour: String,
                      var ts: Long
                     )
