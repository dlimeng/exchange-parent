package com.knowlegene.parent.process.swap;

import com.knowlegene.parent.config.common.constantenum.DBOperationEnum;
import com.knowlegene.parent.config.common.event.FileExportType;
import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.pojo.SwapOptions;
import com.knowlegene.parent.process.pojo.file.FileOptions;
import com.knowlegene.parent.process.swap.event.FileExportTaskEvent;
import com.knowlegene.parent.process.transform.TypeConversion;
import com.knowlegene.parent.process.util.SqlUtil;
import com.knowlegene.parent.scheduler.event.EventHandler;
import com.knowlegene.parent.scheduler.utils.CacheManager;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * @Author: limeng
 * @Date: 2019/8/26 19:36
 */
public class FileExportJob extends ExportJobBase {

    private static volatile FileOptions fileOptions= null;

    public FileExportJob() {
    }

    public FileExportJob(SwapOptions options) {
        super(options);
    }

    public static FileOptions getDbOptions(){
        if(fileOptions == null){
            String name = DBOperationEnum.FILE_EXPORT.getName();
            Object options = getOptions(name);
            if(options != null){
                fileOptions = (FileOptions)options;
            }
        }
        return fileOptions;
    }




    private static PCollection<String> queryByFile(){
        String fieldDelim = getDbOptions().getFieldDelim();
        String filePath = getDbOptions().getFilePath();
        if(fieldDelim == null || fieldDelim == "" || BaseUtil.isBlank(filePath)){
            getLogger().error("fieldDelim/filePath is null");
            return null;
        }

        return getPipeline().apply(TextIO.read().from(filePath).withHintMatchesManyFiles());
    }

    /**
     *
     * 转换 根据导入库的类型
     * @return
     */
    private static PCollection<Row> transformPCollection(){
        PCollection<Row> result = null;

        String[] fieldTitle = getDbOptions().getFieldTitle();
        Schema type = SqlUtil.getSchemaByTitle(fieldTitle);
        if(type == null){
            getLogger().error("schema is null");
            return result;
        }
        String fieldDelim = getDbOptions().getFieldDelim();
        PCollection<String> files = queryByFile();
        if(files!=null){
            return files.apply(ParDo.of(new TypeConversion.StringAndRow(type,fieldDelim))).setCoder(SchemaCoder.of(type));
        }else{
            getLogger().info("files is null");
        }
        return null;
    }





    public static PCollection<Row> query(){
        return transformPCollection();
    }



    public static class FileExportDispatcher implements EventHandler<FileExportTaskEvent> {
        @Override
        public void handle(FileExportTaskEvent event) {
            if (event.getType() == FileExportType.T_EXPORT) {
                getLogger().info("FileExportDispatcher is start");

                PCollection<Row> rows = query();
                CacheManager.setCache(DBOperationEnum.PCOLLECTION_QUERYS.getName(), rows);

            }
        }
    }

}
