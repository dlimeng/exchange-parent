package com.knowlegene.parent.process.swap;

import com.knowlegene.parent.config.common.constantenum.DBOperationEnum;
import com.knowlegene.parent.config.common.event.FileImportType;
import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.config.util.HdfsFileUtil;
import com.knowlegene.parent.process.pojo.ObjectCoder;
import com.knowlegene.parent.process.pojo.SwapOptions;
import com.knowlegene.parent.process.pojo.file.FileOptions;
import com.knowlegene.parent.process.swap.event.FileImportTaskEvent;
import com.knowlegene.parent.process.transform.TypeConversion;
import com.knowlegene.parent.scheduler.event.EventHandler;
import com.knowlegene.parent.scheduler.utils.CacheManager;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.Map;


/**
 * 文件导入
 * @Author: limeng
 * @Date: 2019/8/20 18:58
 */
public class FileImportJob  extends ImportJobBase{

    private static volatile FileOptions fileOptions= null;

    public FileImportJob() {
    }

    public FileImportJob(SwapOptions opts) {
        super(opts);
    }


    public static FileOptions getDbOptions(){
        if(fileOptions == null){
            String name = DBOperationEnum.FILE_IMPORT.getName();
            Object options = getOptions(name);
            if(options != null){
                fileOptions = (FileOptions)options;
            }
        }
        return fileOptions;
    }

    /**
     * 保存文件
     * @param rows
     */
    private static void saveByFile(PCollection<Map<String, ObjectCoder>> rows){
        if(rows != null){
            String filePath = getDbOptions().getFilePath();
            String fieldDelim = getDbOptions().getFieldDelim();
            if(fieldDelim == null || fieldDelim == ""){
                fieldDelim = "\t";
            }
            if(BaseUtil.isBlank(filePath)){
                getLogger().info("filePath is null");
                return;
            }
            KV<String, String> splitKv = HdfsFileUtil.splitMark(filePath, "\\.");

            //切分路径
            PCollection<String> apply = rows.apply(ParDo.of(new TypeConversion.MapAndString(fieldDelim)));
            apply.apply(TextIO.write().to(splitKv.getKey()).withSuffix(splitKv.getValue()));
        }
    }



    public static void save(PCollection<Map<String, ObjectCoder>> rows) {
        if(rows!=null){
           saveByFile(rows);
        }
    }



    public static class FileImportDispatcher implements EventHandler<FileImportTaskEvent> {
        @Override
        public void handle(FileImportTaskEvent event) {
            if(event.getType() == FileImportType.T_IMPORT){
                getLogger().info("FileImportDispatcher is start");

                if(CacheManager.isExist(DBOperationEnum.PCOLLECTION_QUERYS.getName())){
                    save((PCollection<Map<String, ObjectCoder>>)CacheManager.getCache(DBOperationEnum.PCOLLECTION_QUERYS.getName()));
                }

            }
        }
    }

}
