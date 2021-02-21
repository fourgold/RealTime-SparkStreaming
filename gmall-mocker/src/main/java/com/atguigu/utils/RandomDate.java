package com.atguigu.utils;

import java.util.Date;
import java.util.Random;

/**
 * 给一个开始日期,给一个结束日期,然后随机生成中间日期
 * end-start
 * 随机取一个数+start
 */
public class RandomDate {

    private Long logDateTime = 0L;
    private int maxTimeStep = 0;

    public RandomDate(Date startDate, Date endDate, int num) {
        Long avgStepTime = (endDate.getTime() - startDate.getTime()) / num;
        this.maxTimeStep = avgStepTime.intValue() * 2;
        this.logDateTime = startDate.getTime();
    }

    public Date getRandomDate() {
        int timeStep = new Random().nextInt(maxTimeStep);
        logDateTime = logDateTime + timeStep;
        return new Date(logDateTime);
    }
}

