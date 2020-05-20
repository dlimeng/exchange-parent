package com.knowlegene.parent.process.extract.dag.hive;

import com.knowlegene.parent.config.pojo.sql.SQLResult;
import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.extract.BaseExtract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * ddl 定义类
 * @Author: limeng
 * @Date: 2019/8/13 14:58
 */
public abstract class BaseDDL extends BaseExtract implements BaseETLDefaultMethod {

    private final Logger logger;

    public BaseDDL() {
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
     * ddl
     */
    @Override
    public void ddl(){
        List<String> jobs = getResults();
        SQLResult sqlResult = getResult();

        if(!BaseUtil.isBlankSet(jobs)){
            this.getExtractHive().ddlByList(jobs,isOrder());
        }else if(sqlResult != null){
            List<String> ddls = sqlResult.getDdls();
            if(!BaseUtil.isBlankSet(ddls)){
                this.getExtractHive().ddlByList(ddls,isOrder());
            }
        }else{
            getLogger().error("ddl is null");
        }
    }

    @Override
    public void etl() {

    }

}
