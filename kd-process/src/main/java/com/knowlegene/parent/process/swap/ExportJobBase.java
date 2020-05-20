package com.knowlegene.parent.process.swap;

import com.knowlegene.parent.process.model.SwapOptions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * @Author: limeng
 * @Date: 2019/8/20 15:52
 */
public class ExportJobBase extends JobBase implements ExportJob{
    public ExportJobBase() {
    }

    public ExportJobBase(SwapOptions options) {
        super(options);
    }
    /**
     * 注册
     * 导入
     */
    public void runExport(){
        PCollection<Row> query = null;
        if(isHiveExport()){
            query = getExportJobBase(HiveExportJob.class).query();
            if(isFilePath()){
                getExportJobBase(FileExportJob.class).save(query);
            }else if(isEs()){
                getExportJobBase(ESExportJob.class).save(query);
            }else if(isNeo4j()){
                getExportJobBase(Neo4jExportJob.class).save(query);
            }
        }else if(isGbase()){
            query = getExportJobBase(GbaseExportJob.class).query();
            if(isEs()){
                getExportJobBase(ESExportJob.class).save(query);
            }
        }else if(isMySQL()){
            query = getImportJobBase(MySQLImportJob.class).query();
            if(isEs()){
                getExportJobBase(ESExportJob.class).save(query);
            }
        }
    }

    @Override
    public PCollection<Row> query() {
        return null;
    }

    @Override
    public void save(PCollection<Row> rows) {

    }
}
