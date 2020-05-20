package com.knowlegene.parent.process.swap;


import com.knowlegene.parent.process.model.SwapOptions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * import
 * @Author: limeng
 * @Date: 2019/8/20 15:50
 */
public class ImportJobBase extends JobBase implements ImportJob{
    public ImportJobBase() {
    }
    public ImportJobBase(SwapOptions opts) {
        super(opts);
    }


    /**
     * 注册
     * 导入
     */
    public void runImport(){
        PCollection<Row> querys=null;
        //文件导入 -> mysql hive
        if(isFilePath()){
            querys = getImportJobBase(FileImportJob.class).query();
            if(isMySQL()){
               getLogger().info("file->mysql");
               getImportJobBase(MySQLImportJob.class).save(querys);
            }
            if(isHiveImport()){
                getLogger().info("file->hive");
                getImportJobBase(HiveImportJob.class).save(querys);
            }

        }else{
            //数据导入 -> hive
            if(isMySQL()){
                querys = getImportJobBase(MySQLImportJob.class).query();
            }else if(isOracle()){
                querys = getImportJobBase(OracleImportJob.class).query();
            }else if(isEs()){
                querys = getImportJobBase(ESImportJob.class).query();
            }

            if(isHiveImport()){
                getImportJobBase(HiveImportJob.class).save(querys);
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
