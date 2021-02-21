package com.atguigu.gmallpublisher.service;

/**
 * @author Jinxin Li
 * @create 2020-12-02 10:46
 */
import java.util.Map;

public interface PublisherService {

    public Integer getDauTotal(String date);

    public Map getDauTotalHourMap(String date);

    public Double getOrderAmount(String data);

    public Map getOrderAmountHour(String data);

}
