package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author Jinxin Li
 * @create 2020-12-04 11:53
 */
public interface OrderMapper {
    //总交易额
    public Double selectOrderAmountTotal(String date);
    //分时总交易额
    public List<Map> selectOrderAmountHourMap(String date);
}
