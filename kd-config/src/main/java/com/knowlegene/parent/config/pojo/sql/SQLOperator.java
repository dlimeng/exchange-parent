package com.knowlegene.parent.config.pojo.sql;

import java.util.List;

/**
 * @Author: limeng
 * @Date: 2019/8/8 18:59
 */
public interface SQLOperator {
    public enum Operator {
        DDL, CREATE, INSERT
    }
    public List<String> listBuilders();
}
