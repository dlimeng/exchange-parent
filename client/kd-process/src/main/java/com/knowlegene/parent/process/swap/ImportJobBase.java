package com.knowlegene.parent.process.swap;


import com.knowlegene.parent.process.pojo.SwapOptions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * import
 * @Author: limeng
 * @Date: 2019/8/20 15:50
 */
public class ImportJobBase extends JobBase {



    public ImportJobBase() {
    }
    public ImportJobBase(SwapOptions opts) {
        super(opts);
    }





    public static PCollection<Row> query() {
        return null;
    }


    public static void save(PCollection<Row> rows) { }
}
