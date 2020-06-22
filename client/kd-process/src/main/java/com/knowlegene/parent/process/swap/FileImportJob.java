package com.knowlegene.parent.process.swap;

import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.pojo.SwapOptions;
import com.knowlegene.parent.process.transform.TypeConversion;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * 文件导入
 * @Author: limeng
 * @Date: 2019/8/20 18:58
 */
public class FileImportJob  extends ImportJobBase{

    public FileImportJob() {
    }

    public FileImportJob(SwapOptions opts) {
        super(opts);
    }

    public PCollection<String> queryByFile(){
//        String fieldDelim = options.getFieldDelim();
//        String filePath = options.getFilePath();
//        if(fieldDelim == null || fieldDelim == "" || BaseUtil.isBlank(filePath)){
//            getLogger().error("fieldDelim/filePath is null");
//            return null;
//        }
//
//        return super.getPipeline().apply(TextIO.read().from(filePath).withHintMatchesManyFiles());
        return null;
    }

    /**
     *
     * 转换 根据导入库的类型
     * @return
     */
    public PCollection<Row> transformPCollection(){
//        PCollection<Row> result = null;
//        Schema type= null;
//        if(isMySQL()){
//            type = getMysqlSchemas();
//        }else if(isHiveImport()){
//            type = getHiveSchemas(true);
//        }
//        if(type == null){
//            getLogger().error("schema is null");
//            return result;
//        }
//        String fieldDelim = options.getFieldDelim();
//        PCollection<String> files = queryByFile();
//        if(files!=null){
//            return files.apply(ParDo.of(new TypeConversion.StringAndRow(type,fieldDelim))).setCoder(SchemaCoder.of(type));
//        }else{
//            getLogger().info("files is null");
//        }
        return null;

    }





    public static PCollection<Row> query(){

        //return transformPCollection();
        return null;
    }
}
