package com.knowlegene.parent.process.swap;

import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.config.util.HdfsFileUtil;
import com.knowlegene.parent.process.pojo.SwapOptions;
import com.knowlegene.parent.process.transform.TypeConversion;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * @Author: limeng
 * @Date: 2019/8/26 19:36
 */
public class FileExportJob extends ExportJobBase {
    public FileExportJob() {
    }

    public FileExportJob(SwapOptions options) {
        super(options);
    }


    public static void save(PCollection<Row> rows) {
//        if(isHiveExport() && rows!=null){
//           saveByFile(rows);
//        }
    }

    /**
     * 保存文件
     * @param rows
     */
    public void saveByFile(PCollection<Row> rows){
        if(rows != null){
            String filePath = options.getFilePath();
            String fieldDelim = options.getFieldDelim();
            if(fieldDelim == null || fieldDelim == ""){
                fieldDelim = "\t";
            }
            if(BaseUtil.isBlank(filePath)){
                getLogger().info("filePath is null");
                return;
            }
            KV<String, String> splitKv = HdfsFileUtil.splitMark(filePath, "\\.");

            //切分路径
            PCollection<String> apply = rows.apply(ParDo.of(new TypeConversion.RowAndString(fieldDelim)));
            apply.apply(TextIO.write().to(splitKv.getKey()).withSuffix(splitKv.getValue()));
        }
    }




}
