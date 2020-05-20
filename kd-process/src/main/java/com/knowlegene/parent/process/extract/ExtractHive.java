package com.knowlegene.parent.process.extract;

import com.knowlegene.parent.config.common.constantenum.HiveTypeEnum;
import com.knowlegene.parent.process.runners.options.IndexerPipelineOptions;
import com.knowlegene.parent.config.pojo.sql.SQLResult;
import com.knowlegene.parent.config.util.BaseSqlParserFactoryUtil;
import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.config.util.PipelineSingletonUtil;
import com.knowlegene.parent.process.common.constantenum.DataSourceEnum;
import com.knowlegene.parent.process.io.file.HadoopFile;
import com.knowlegene.parent.process.io.jdbc.HiveChange;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.Resource;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @Author: limeng
 * @Date: 2019/8/12 10:20
 */
public class ExtractHive {
    private Logger logger = LoggerFactory.getLogger(this.getClass());


    @Resource
    private HiveChange hiveChange;
    @Resource
    private HadoopFile hadoopFile;


    /**
     * 分配链路
     * @param  pipelineOptions 参数
     */
    public void switchingOperationList(IndexerPipelineOptions pipelineOptions) {
        if(pipelineOptions == null){
            return;
        }
        Boolean sqlFile = pipelineOptions.isSqlFile();
        //是否有序
        Boolean orderLink = pipelineOptions.isOrderLink();
        List<String> conversions = null;
        if(sqlFile){
            String filePath = pipelineOptions.getFilePath();
            conversions = hadoopFile.readFile(filePath);
        }else{
            String jdbcSql = pipelineOptions.getJdbcSql();
            if(BaseUtil.isBlank(jdbcSql)){
                return;
            }
            conversions = Collections.singletonList(jdbcSql);
        }

        if(BaseUtil.isBlankSet(conversions)){
            return;
        }
        if(orderLink){
            //有序
            this.orderLink(conversions,true);
        }else{
            //ddl操作
            this.ddlByList(conversions);
            hiveChange.saveByOrder(conversions,orderLink);
        }
    }

    /**
     * jdbc基础操作
     * @param sql
     */
    public int create(String sql) {
        return hiveChange.create(sql);
    }

    /**
     * ddl操作
     * @param sqls
     */
    public void ddlByList(List<String> sqls) {
        if(!BaseUtil.isBlankSet(sqls)){
            HiveTypeEnum create1 = HiveTypeEnum.CREATE1;
            HiveTypeEnum drop1 = HiveTypeEnum.DROP1;
            HiveTypeEnum set1 = HiveTypeEnum.SET1;
            int value = DataSourceEnum.HIVE.getValue();
            boolean ddlStatus=false;
            Iterator<String> iterator = sqls.iterator();

            while (iterator.hasNext()){
                String sql = iterator.next();
                int status = BaseSqlParserFactoryUtil.generateParser(value, sql);
                ddlStatus = (status == create1.getValue() || status==drop1.getValue() || status==set1.getValue());
                if(ddlStatus){
                    int result = this.create(sql);
                    iterator.remove();
                    logger.info("ddl=>sql:{},rowsAffected:{}",sql,result);
                }
            }
        }

    }

    public List<String> ddlByList(List<String> sqls,boolean isOreder) {
        return hiveChange.ddlByOrder(sqls,isOreder);
    }

    public PCollection<String> ddlByPCollection(PCollection<String> sql){
        return hiveChange.ddl(sql);
    }

    /**
     * jdbcio write 保存
     * @param sql
     */
    public void save(String sql) {
        Schema type =
                Schema.builder().addStringField("isNull").build();
        Row build = Row.withSchema(type).addValue("true").build();
        hiveChange.save(sql,build);
    }

    public  PCollection<String> saveByPCollection(PCollection<String> sql){
        return hiveChange.save(sql);
    }

    /**
     * 删除
     * @param sql
     */
    public void delete(String sql) {
        hiveChange.delete(sql);
    }

    /**
     * 有序链路
     * @param sqls
     */
    public void orderLink(List<String> sqls,boolean isOreder) {
        if(!BaseUtil.isBlankSet(sqls)){
            hiveChange.ddlByOrder(sqls,isOreder);
            hiveChange.saveByOrder(sqls, isOreder);
        }
    }


    /**
     * 保存
     */
    public List<String>  saveByList(List<String> sqls,boolean isOreder){
        List<String> result=null;
        if(!BaseUtil.isBlankSet(sqls)){
            result = hiveChange.saveByOrder(sqls, isOreder);
        }
        return result;
    }

    /**
     * 保存
     * @param resultDO
     */
    public void saveByOrder(SQLResult resultDO){
        hiveChange.saveByOrder(resultDO);
    }

    /**
     * ddl
     * @param resultDO
     */
    public void ddlByOrder(SQLResult resultDO){
        hiveChange.ddlByOrder(resultDO);
    }

    /**
     * 无序分发任务
     * @param pCollections
     * @return
     */
    public PCollection<String> distributionLink(PCollection<String> pCollections){
        return hiveChange.distributionLink(pCollections);
    }

    /**
     * 判断传参状态
     * 分发任务
     * 是否有序
     * 是否有文件
     * 文件地址
     * sql
     */
    public void distributionTask() {
        IndexerPipelineOptions pipelineOptions = getOptions();
        if(pipelineOptions == null){
            logger.error("pipelineOptions is null");
            return;
        }
        this.switchingOperationList(pipelineOptions);
    }

    /**
     * 获取参数
     * @return
     */
    private IndexerPipelineOptions getOptions(){
        Pipeline instance = PipelineSingletonUtil.instance;
        if(instance == null){
            logger.error("pipeline is null");
            return null;
        }
        return (IndexerPipelineOptions) instance.getOptions();
    }

}
