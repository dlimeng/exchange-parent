package com.knowlegene.parent.process.swap;

import com.knowlegene.parent.config.common.constantenum.HiveTypeEnum;
import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.config.util.JdbcUtil;
import com.knowlegene.parent.process.model.SwapOptions;
import com.knowlegene.parent.process.transform.TypeConversion;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.hive.hcatalog.data.HCatRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * hive导出
 * @Author: limeng
 * @Date: 2019/8/20 16:50
 */
public class HiveExportJob extends ExportJobBase{

    public HiveExportJob() {
    }

    public HiveExportJob(SwapOptions options) {
        super(options);
    }


    private PCollection<Row> getHCatRecordAndRow(PCollection<HCatRecord> ps){
        if(ps != null){
            String tableName = options.getHiveTableName();
            Schema allSchema = this.getHiveSwap().descByTableName(tableName,true);
            if(allSchema !=null){
                return ps.apply(ParDo.of(new TypeConversion.HCatRecordAndRow(allSchema))).setCoder(SchemaCoder.of(allSchema));
            }else{
                getLogger().error("allSchema is null");
            }
        }
        return null;
    }
    /**
     * 查询表
     * @return
     */
    public PCollection<Row> queryByTable(){
        String[] dbColumn = options.getHiveColumn();
        String tableName = options.getHiveTableName();

        //表所有列
        Schema allSchema = this.getHiveSwap().descByTableName(tableName,true);
        Schema schema = JdbcUtil.columnConversion(dbColumn, allSchema);
        if(schema == null){
            getLogger().error("schema is null");
            return null;
        }
        getLogger().info("tableName:{}",tableName);
                return super.getPipeline().apply(this.getHiveSwap().queryByTable(tableName))
        .apply(ParDo.of(new TypeConversion.MapObjectAndRow(schema))).setCoder(SchemaCoder.of(schema));
    }

    /**
     * 查询
     */
    public PCollection<HCatRecord> queryByHCatalog(){
        String uris = HiveTypeEnum.HCATALOGMETASTOREURIS.getName();
        String db= HiveTypeEnum.HIVEDATABASE.getName();
        String table=HiveTypeEnum.HIVETABLE.getName();
        Map<String, String> configProperties = new HashMap<>();
        String metastoreHostName = options.getHMetastoreHost();
        String metastorePort = options.getHMetastorePort();
        String hiveDatabase = options.getHiveDatabase();
        String hiveTableName = options.getHiveTableName();

        //hCatio参数
        if(BaseUtil.isBlank(metastoreHostName) || BaseUtil.isBlank(metastorePort) || BaseUtil.isBlank(hiveDatabase) || BaseUtil.isBlank(hiveTableName)){
            return null;
        }


        String uriValue = String.format("thrift://%s:%s",metastoreHostName,metastorePort);
        configProperties.put(uris,uriValue);
        configProperties.put(db,hiveDatabase);
        configProperties.put(table,hiveTableName);
        getLogger().info("tableName:{}",hiveTableName);
        String hiveFilter = options.getHiveFilter();

        if(BaseUtil.isNotBlank(hiveFilter)){
            return super.getPipeline()
                    .apply(this.getHiveSwap().queryByHCatalogIO(configProperties,hiveFilter));
        }else{
            return super.getPipeline()
                    .apply(this.getHiveSwap().queryByHCatalogIO(configProperties));
        }
    }


    @Override
    public PCollection<Row> query() {
        PCollection<Row> result=null;
        if(isHCatalogIOStatus()){
            PCollection<HCatRecord> ps = queryByHCatalog();
            result = getHCatRecordAndRow(ps);
        }else{
            result = queryByTable();
        }
        return result;
    }
}
