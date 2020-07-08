package com.knowlegene.parent.process.swap;


import com.knowlegene.parent.process.pojo.ObjectCoder;
import com.knowlegene.parent.process.pojo.SwapOptions;
import org.apache.beam.sdk.values.PCollection;

import java.util.Map;

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





    public static PCollection<Map<String, ObjectCoder>> query() {
        return null;
    }


    public static void save(PCollection<Map<String, ObjectCoder>> rows) { }
}
