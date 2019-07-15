package com.knowlegene.parent.process.model;

import lombok.Data;

import java.io.Serializable;
import java.util.Objects;

/**
 * @Author: limeng
 * @Date: 2019/7/15 10:51
 */
@Data
public class TestRow implements Serializable, Comparable<TestRow>  {

    private String id;
    private String name;

    public TestRow() {
    }

    public TestRow(String id, String name) {
        this.id = id;
        this.name = name;
    }
    @Override
    public int compareTo(TestRow other) {
        return name.compareTo(other.name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TestRow tr = (TestRow) o;
        return id == tr.id
                && Objects.equals(name, tr.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name);
    }
}
