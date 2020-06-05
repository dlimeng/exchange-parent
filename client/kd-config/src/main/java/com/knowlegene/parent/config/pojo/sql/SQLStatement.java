package com.knowlegene.parent.config.pojo.sql;


import com.knowlegene.parent.config.pojo.BaseDO;
import lombok.Data;

/**
 * @Author: limeng
 * @Date: 2019/8/8 19:24
 */
@Data
public class SQLStatement extends BaseDO {
    private String sql;

    public SQLStatement() {

    }

}
