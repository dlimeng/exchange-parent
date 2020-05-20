package com.knowlegene.parent.process.util;

import com.knowlegene.parent.config.common.constantenum.DatabaseTypeEnum;
import com.knowlegene.parent.config.common.constantenum.HiveTypeEnum;
import com.knowlegene.parent.config.util.BaseSqlParserFactoryUtil;
import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.config.util.HSqlThreadLocalUtil;
import com.knowlegene.parent.config.util.JdbcUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: limeng
 * @Date: 2019/8/15 23:30
 */
public class SqlUtil {
    private static Logger logger = LoggerFactory.getLogger(SqlUtil.class);
    /**
     * 全局
     * hive set变量
     */
    public static void setHiveVariable(List<String> sqls){
        if(!BaseUtil.isBlankSet(sqls)){
            HiveTypeEnum set1 = HiveTypeEnum.SET1;
            int value = DatabaseTypeEnum.HIVE.getValue();
            List<String> result=new ArrayList<>();
            for (String sql:sqls){
                int status = BaseSqlParserFactoryUtil.generateParser(value, sql);
                if(status == set1.getValue()){
                    result.add(sql);
                }
            }
            HSqlThreadLocalUtil.setJob(result);
        }
    }



    /**
     * row格式转换HCatSchema
     * @return
     */
    public static HCatSchema getRowAndHCatSchema(Schema schema) throws HCatException {
        if(schema != null){
            List<HCatFieldSchema> columns = new ArrayList<>();
            List<Schema.Field> fields = schema.getFields();
            if(!BaseUtil.isBlankSet(fields)){
                Schema.TypeName typeName = null;
                for(Schema.Field field:fields){
                    typeName = field.getType().getTypeName();
                    if(typeName == Schema.TypeName.STRING){
                        columns.add(new HCatFieldSchema(field.getName(), TypeInfoFactory.stringTypeInfo, ""));
                    }else if(typeName == Schema.TypeName.INT16){
                        columns.add(new HCatFieldSchema(field.getName(), TypeInfoFactory.shortTypeInfo, ""));
                    }else if(typeName == Schema.TypeName.INT32){
                        columns.add(new HCatFieldSchema(field.getName(), TypeInfoFactory.intTypeInfo, ""));
                    }else if(typeName == Schema.TypeName.INT64){
                        columns.add(new HCatFieldSchema(field.getName(), TypeInfoFactory.longTypeInfo, ""));
                    }else if(typeName == Schema.TypeName.FLOAT){
                        columns.add(new HCatFieldSchema(field.getName(), TypeInfoFactory.floatTypeInfo, ""));
                    }else if(typeName == Schema.TypeName.DOUBLE){
                        columns.add(new HCatFieldSchema(field.getName(), TypeInfoFactory.doubleTypeInfo, ""));
                    }else if(typeName == Schema.TypeName.BOOLEAN){
                        columns.add(new HCatFieldSchema(field.getName(), TypeInfoFactory.booleanTypeInfo, ""));
                    }else if(typeName == Schema.TypeName.DATETIME){
                        columns.add(new HCatFieldSchema(field.getName(), TypeInfoFactory.timestampTypeInfo, ""));
                    }else if(typeName == Schema.TypeName.BYTES){
                        columns.add(new HCatFieldSchema(field.getName(), TypeInfoFactory.byteTypeInfo, ""));
                    }else if(typeName == Schema.TypeName.DECIMAL){
                        columns.add(new HCatFieldSchema(field.getName(), TypeInfoFactory.decimalTypeInfo, ""));
                    }
                }

                if(!BaseUtil.isBlankSet(columns)){
                    return new HCatSchema(columns);
                }
            }
        }
        return null;
    }

}
