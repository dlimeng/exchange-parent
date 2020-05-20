package com.knowlegene.parent.process.extract.dag.hive;

import com.knowlegene.parent.config.common.constantenum.DatabaseTypeEnum;
import com.knowlegene.parent.config.common.constantenum.HiveTypeEnum;
import com.knowlegene.parent.config.pojo.sql.SQLResult;
import com.knowlegene.parent.config.util.BaseSqlParserFactoryUtil;
import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.config.util.HSqlThreadLocalUtil;
import com.knowlegene.parent.process.extract.BaseExtract;
import com.knowlegene.parent.process.util.SqlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * etl公共类
 * @Author: limeng
 * @Date: 2019/8/13 14:58
 */
public abstract class BaseETL extends BaseExtract implements BaseETLDefaultMethod {
    private final Logger logger;

    public BaseETL() {
        this.logger = LoggerFactory.getLogger(getClass());
    }

    public Logger getLogger() {
        return logger;
    }

    public abstract List<String> getResults();

    public abstract SQLResult getResult();

    /**
     * 是否有序
     * @return
     */
    public boolean isOrder(){
        return true;
    }

    /**
     * @param sqls
     * @return
     */
    public List<String> getCleanValue(List<String> sqls){
        HSqlThreadLocalUtil.clearJob();
        List<String> result =new ArrayList<>();
        List<String> setVariable=new ArrayList<>();
        if(!BaseUtil.isBlankSet(sqls)){
            HiveTypeEnum set1 = HiveTypeEnum.SET1;
            int value = DatabaseTypeEnum.HIVE.getValue();
            for(String sql:sqls){
                int status = BaseSqlParserFactoryUtil.generateParser(value, sql);
                //设置全局hive set
                if(status == set1.getValue()){
                    setVariable.add(sql);
                }else{
                    result.add(sql);
                }
            }
        }
        SqlUtil.setHiveVariable(setVariable);
        return result;
    }
    /**
     * etl
     */
    @Override
    public void etl(){
        List<String> jobs = getResults();
        SQLResult sqlResult = getResult();
        List<String> result=null;
        if(!BaseUtil.isBlankSet(jobs)){
            result = this.getCleanValue(jobs);
        }else if(sqlResult != null){
            List<String> inserts = sqlResult.getInserts();
            if(!BaseUtil.isBlankSet(inserts)){
                result = this.getCleanValue(inserts);
            }
        }else{
            logger.error("etl is null");
        }
        this.getExtractHive().saveByList(result,isOrder());
    }

    @Override
    public void ddl() {

    }

}
