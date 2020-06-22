package com.knowlegene.parent.process.pojo;

import lombok.Data;

import java.io.Serializable;

/**
 * @Author: limeng
 * @Date: 2019/9/16 17:23
 */
@Data
public class ObjectCoder implements Serializable {
    private Object value;

    public ObjectCoder(Object value) {
        this.value = value;
    }
}
