package com.knowlegene.parent.process.pojo;

import lombok.Data;
import org.apache.beam.sdk.schemas.Schema;

import java.io.Serializable;

/**
 * @Author: limeng
 * @Date: 2019/9/16 17:23
 */
@Data
public class ObjectCoder implements Serializable {
    private Object value;
    private Schema.FieldType fieldType;
    private Integer index;

    public ObjectCoder(Object value, Schema.FieldType fieldType, Integer index) {
        this.value = value;
        this.fieldType = fieldType;
        this.index = index;
    }

    public ObjectCoder(Object value, Schema.FieldType fieldType) {
        this.value = value;
        this.fieldType = fieldType;
    }

    public ObjectCoder(Object value, Integer index) {
        this.value = value;
        this.index = index;
    }

    public ObjectCoder(Object value) {
        this.value = value;
    }

    public ObjectCoder() {
    }
}
