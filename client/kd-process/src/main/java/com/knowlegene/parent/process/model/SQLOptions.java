package com.knowlegene.parent.process.model;

import com.knowlegene.parent.config.pojo.BaseDTO;
import lombok.Data;

/**
 * @Author: limeng
 * @Date: 2019/8/1 14:28
 */
@Data
public class SQLOptions extends BaseDTO {
    /**
     * 操作类型
     */
    private String optionType;
    private String select;
    private String from;
    private String where;
    private String join;
    private String insert;
    /**
     * set
     * with
     * create
     * truncate
     */
    private String ddl;
}
