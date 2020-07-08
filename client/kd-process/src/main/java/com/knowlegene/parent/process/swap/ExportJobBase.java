package com.knowlegene.parent.process.swap;


import com.knowlegene.parent.process.pojo.ObjectCoder;
import com.knowlegene.parent.process.pojo.SwapOptions;
import org.apache.beam.sdk.values.PCollection;


import java.util.Map;

/**
 * @Author: limeng
 * @Date: 2019/8/20 15:52
 */
public class ExportJobBase extends JobBase {


    public ExportJobBase() {
    }

    public ExportJobBase(SwapOptions options) {
        super(options);
    }




    public static PCollection<Map<String, ObjectCoder>> query() {
        return null;
    }


    public static void save(PCollection<Map<String, ObjectCoder>> rows) {

    }
}
