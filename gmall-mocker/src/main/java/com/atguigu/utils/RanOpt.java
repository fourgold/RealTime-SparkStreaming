package com.atguigu.utils;

/**
 * 实现权重分配选择
 * 使用一个随机选项组
 * @param <T>
 */
public class RanOpt<T> {

    private T value;
    private int weight;

    public RanOpt(T value, int weight) {
        this.value = value;
        this.weight = weight;
    }

    public T getValue() {
        return value;
    }

    public int getWeight() {
        return weight;
    }
}
