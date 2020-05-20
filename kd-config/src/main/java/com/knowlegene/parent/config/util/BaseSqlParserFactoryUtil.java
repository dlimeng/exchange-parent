package com.knowlegene.parent.config.util;

import com.knowlegene.parent.config.common.constantenum.DatabaseTypeEnum;
import com.knowlegene.parent.config.common.constantenum.HiveTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 解析sql
 * @Author: limeng
 * @Date: 2019/7/22 21:37
 */
public class BaseSqlParserFactoryUtil {
    private static Logger logger = LoggerFactory.getLogger(BaseSqlParserFactoryUtil.class);
    /**
     * 查询
     */
    private static final String SELECT1 ="^(select)(.+)(from)(.+)";

    private static final String SAVE1 = "^(insert)(.+)";
    private static final String DELETE1="(delete from)(.+)";
    private static final String CREATE1="(create)(.+)";
    private static final String TRUNCATE1="(truncate)(.+)";
    private static final  String LOAD1="^(load)(.+)";

    public static int generateParser(Integer source,String originalSql) {
        if(source == null){
            return -1;
        }
        HiveTypeEnum select1 = HiveTypeEnum.SELECT1;
        HiveTypeEnum save1 = HiveTypeEnum.SAVE1;
        HiveTypeEnum delete1 = HiveTypeEnum.DELETE1;

        HiveTypeEnum create1 = HiveTypeEnum.CREATE1;
        HiveTypeEnum truncate1 = HiveTypeEnum.TRUNCATE1;
        HiveTypeEnum load1 = HiveTypeEnum.LOAD1;
        HiveTypeEnum drop1 = HiveTypeEnum.DROP1;
        HiveTypeEnum set1 = HiveTypeEnum.SET1;


        boolean sourceBoolean = source.equals(DatabaseTypeEnum.HIVE.getValue());
        if(sourceBoolean && contains(originalSql,select1.getName())) {
            return select1.getValue();
        }else if(sourceBoolean && contains(originalSql,save1.getName())){
            return save1.getValue();
        }else if(sourceBoolean && contains(originalSql,delete1.getName())){
            return delete1.getValue();
        }else if(sourceBoolean && contains(originalSql,create1.getName())){
            return create1.getValue();
        }else if(sourceBoolean && contains(originalSql,truncate1.getName())){
            return truncate1.getValue();
        }else if(sourceBoolean && contains(originalSql,load1.getName())){
            return load1.getValue();
        }else if(sourceBoolean && contains(originalSql,drop1.getName())){
            return drop1.getValue();
        }else if(sourceBoolean && contains(originalSql,set1.getName())){
            return set1.getValue();
        }else {
            logger.info("BaseSqlParserFactory generateParser error=>sql:{}",originalSql);
            return -1;
        }
    }


    /**
     * 包含
     * @param sql sql
     * @param regExp 正则
     * @return
     */
    private static boolean contains(String sql,String regExp){
        Pattern pattern=Pattern.compile(regExp,Pattern.CASE_INSENSITIVE);
        Matcher matcher=pattern.matcher(sql.toLowerCase());
        return matcher.find();
    }


}
