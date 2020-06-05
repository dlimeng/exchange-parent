package com.knowlegene.parent.process.swap;

import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * @Author: limeng
 * @Date: 2019/8/30 15:38
 */
public interface ImportJob {
    PCollection<Row> query();
    void save(PCollection<Row> rows);
}
