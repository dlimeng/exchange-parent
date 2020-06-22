package com.knowlegene.parent.process.route;

import com.alibaba.fastjson.JSON;
import com.knowlegene.parent.process.runners.options.IndexerPipelineOptions;
import com.knowlegene.parent.config.pojo.sql.SQLOperators;
import com.knowlegene.parent.config.pojo.sql.SQLResult;
import com.knowlegene.parent.process.extract.BaseExtract;
import com.knowlegene.parent.process.pojo.SQLOptions;
import com.knowlegene.parent.process.runners.common.BaseRunners;
import org.junit.Test;
import com.knowlegene.parent.process.common.constantenum.OptionsEnum.Indexer;
import java.util.HashMap;
import java.util.Map;

/**
 * 抽取
 * @Author: limeng
 * @Date: 2019/8/12 18:49
 */
public class ExtractHiveTest extends BaseExtract {
    /**
     * 保存
     */
    @Test
    public void save(){
        BaseRunners baseRunners = new BaseRunners();
        baseRunners.start();
        this.getExtractHive().save("insert into a_mart.test values(\"444\",\"text\")");
        baseRunners.run();
    }
    @Test
    public void saveByList(){
        BaseRunners baseRunners = new BaseRunners(IndexerPipelineOptions.class);
        /**
         * 批量读取文件
         * 是否批处理状态sqlFile
         * 是否有序orderLink
         * 是否传语句jdbcSql
         * 是否有文件路径filePath
         */
        Map<String,Object> options = new HashMap<>();
        String sqlFile = Indexer.ISSQLFILE.name;
        String orderLink = Indexer.ISORDERLINK.name;
        String jdbcSql=Indexer.JDBCSQL.name;
        String filePath = Indexer.FILEPATH.name;
        options.put(sqlFile,"true");
        options.put(orderLink,"true");
        options.put(filePath,"");
        baseRunners.start(options);
        this.getExtractHive().distributionTask();
        baseRunners.run();
    }
    @Test
    public void saveByOrder(){
        BaseRunners baseRunners = new BaseRunners();
        baseRunners.start();
        String json2="{\"optionType\":\"insert\",\"insert\":\"insert overwrite table  a_mart.test2019\",\"select\":\"select id,name\",\"from\":\"from a_mart.test2019\"}";
        SQLOptions sqlOptions = JSON.parseObject(json2, SQLOptions.class);
        SQLResult sqlResult = new SQLResult();

        sqlResult.setIsOrder(true);
        sqlResult.insert(new SQLOperators().addInsert(sqlOptions.getInsert())
                .addSelect(sqlOptions.getSelect()).addJoin(sqlOptions.getJoin())
                .addFrom(sqlOptions.getFrom()).addWhere(sqlOptions.getWhere()));

        this.getExtractHive().saveByOrder(sqlResult);
        baseRunners.run();

    }


}
