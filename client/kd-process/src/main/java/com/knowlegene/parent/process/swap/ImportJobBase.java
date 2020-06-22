package com.knowlegene.parent.process.swap;


import com.knowlegene.parent.config.common.constantenum.DBOperationEnum;
import com.knowlegene.parent.process.pojo.DBOptions;
import com.knowlegene.parent.process.pojo.SwapOptions;
import com.knowlegene.parent.process.pojo.hive.HiveOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * import
 * @Author: limeng
 * @Date: 2019/8/20 15:50
 */
public class ImportJobBase extends JobBase {
    protected static HiveOptions hiveOptions;


    public ImportJobBase() {
    }
    public ImportJobBase(SwapOptions opts) {
        super(opts);
    }


    public static HiveOptions getHiveOptions(){
        if(hiveOptions == null){
            String name = DBOperationEnum.HIVE_IMPORT.getName();
            Object options = getOptions(name);
            if(options != null){
                hiveOptions = (HiveOptions)options;
            }
        }
        return hiveOptions;
    }


    public static PCollection<Row> query() {
        return null;
    }


    public static void save(PCollection<Row> rows) { }
}
