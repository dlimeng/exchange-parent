package com.knowlegene.parent.config.common.constantenum;

/**
 * 日期正则
 * @Author: limeng
 * @Date: 2019/7/23 23:51
 */
public enum TimeTypeEnum {
    /**
     *
     */
    YMDHMSS(1,"yyyy-MM-dd HH:mm:ss.SSS"),YMDHMS(2,"yyyy-MM-dd HH:mm:ss");

    /**
     * hive参数
     */
    /**
     * 类型标记
     */
    private int value;

    /**
     * 类型名称
     */
    private String name;

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    TimeTypeEnum() {
    }

    TimeTypeEnum(int value, String name) {
        this.value = value;
        this.name = name;
    }
}
