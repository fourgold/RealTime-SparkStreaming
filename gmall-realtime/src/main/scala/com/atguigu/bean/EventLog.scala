package com.atguigu.bean

/**
 * @author Jinxin Li
 * @create 2020-12-07 9:13
 */
case class EventLog(mid: String,
                    uid: String,
                    appid: String,
                    area: String,
                    os: String,
                    `type`: String,
                    evid: String,
                    pgid: String,
                    npgid: String,
                    itemid: String,
                    var logDate: String,
                    var logHour: String,
                    var ts: Long)

