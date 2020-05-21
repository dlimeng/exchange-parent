package com.knowlegene.parent.process.common.constantenum;

/**
 * 不同方式 import export
 * @Author: limeng
 * @Date: 2019/7/23 23:36
 */
public enum PatternEnum {
    /**
     * 各个源
     */
    IMPORT("import","导入"),EXPORT("export","导出");

    /**
     * 类型标记
     */
    private String value;

    /**
     * 类型名称
     */
    private String name;

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    PatternEnum() {
    }

    PatternEnum(String value, String name) {
        this.value = value;
        this.name = name;
    }
}
