package com.knowlegene.parent.process.pojo;

import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.ReaderWriter;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @Author: limeng
 * @Date: 2019/8/30 17:56
 */
public class DefaultHCatRecord extends HCatRecord implements Serializable {
    private List<Object> contents;

    public DefaultHCatRecord() {
        this.contents = new ArrayList();
    }

    public DefaultHCatRecord(int size) {
        this.contents = new ArrayList(size);

        for(int i = 0; i < size; ++i) {
            this.contents.add((Object)null);
        }

    }

    @Override
    public void remove(int idx) throws HCatException {
        this.contents.remove(idx);
    }

    public DefaultHCatRecord(List<Object> list) {
        this.contents = list;
    }

    @Override
    public Object get(int fieldNum) {
        return this.contents.get(fieldNum);
    }

    @Override
    public List<Object> getAll() {
        return this.contents;
    }

    @Override
    public void set(int fieldNum, Object val) {
        this.contents.set(fieldNum, val);
    }

    @Override
    public int size() {
        return this.contents.size();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.contents.clear();
        int len = in.readInt();

        for(int i = 0; i < len; ++i) {
            this.contents.add(ReaderWriter.readDatum(in));
        }

    }

    @Override
    public void write(DataOutput out) throws IOException {
        int sz = this.size();
        out.writeInt(sz);

        for(int i = 0; i < sz; ++i) {
            ReaderWriter.writeDatum(out, this.contents.get(i));
        }

    }

    @Override
    public int hashCode() {
        int hash = 1;
        Iterator i$ = this.contents.iterator();

        while(i$.hasNext()) {
            Object o = i$.next();
            if (o != null) {
                hash = 31 * hash + o.hashCode();
            }
        }

        return hash;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        Iterator i$ = this.contents.iterator();

        while(i$.hasNext()) {
            Object o = i$.next();
            sb.append(o + "\t");
        }

        return sb.toString();
    }

    @Override
    public Object get(String fieldName, HCatSchema recordSchema) throws HCatException {
        return this.get(recordSchema.getPosition(fieldName));
    }

    @Override
    public void set(String fieldName, HCatSchema recordSchema, Object value) throws HCatException {
        this.set(recordSchema.getPosition(fieldName), value);
    }

    @Override
    public void copy(HCatRecord r) throws HCatException {
        this.contents = r.getAll();
    }
}
