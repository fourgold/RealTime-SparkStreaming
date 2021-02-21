package com.atguigu.bean

/**
 * @author Jinxin Li
 * @create 2020-12-07 9:13
 */
case class CouponAlertInfo(mid: String,
                           uids: java.util.HashSet[String],
                           itemIds: java.util.HashSet[String],
                           events: java.util.List[String],
                           ts: Long
                          )
