package com.knowlegene.parent.process.reflect;

import com.knowlegene.parent.process.common.annotation.StoredAsProperty;

/**
 * @Author: limeng
 * @Date: 2019/8/29 17:05
 */
public class Indexer {
    @StoredAsProperty("pattern")
    private String pattern;
    @StoredAsProperty("sourceName")
    private String sourceName;
    private String test;

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public String getTest() {
        return test;
    }

    public void setTest(String test) {
        this.test = test;
    }
}
