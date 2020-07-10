package com.knowlegene.parent.process.pojo;

import lombok.Data;
import org.apache.beam.sdk.schemas.Schema;

import java.io.Serializable;
import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ObjectCoder that = (ObjectCoder) o;
        return Objects.equals(value, that.value) &&
                Objects.equals(fieldType, that.fieldType) &&
                Objects.equals(index, that.index);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, fieldType, index);
    }
}
