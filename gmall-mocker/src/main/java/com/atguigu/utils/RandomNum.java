package com.atguigu.utils;

import java.util.Random;

/**
 * 给一个指定范围产生随机数
 * end-start
 * +1看主要是左边还是右边
 */
public class RandomNum {
    public static int getRandInt(int fromNum, int toNum) {
        return fromNum + new Random().nextInt(toNum - fromNum + 1);
    }
}
