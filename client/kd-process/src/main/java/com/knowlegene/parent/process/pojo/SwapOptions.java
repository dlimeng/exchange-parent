package com.knowlegene.parent.process.pojo;

import com.alibaba.fastjson.JSON;
import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.common.annotation.StoredAsProperty;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 数据交换
 * @Author: limeng
 * @Date: 2019/8/20 14:22
 */
@Data
public class SwapOptions {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    @StoredAsProperty("datasource.from")
    private String fromName;
    @StoredAsProperty("datasource.to")
    private String toName;


    @StoredAsProperty("file.url")
    private String filePath;
    @StoredAsProperty("field.delim")
    private String fieldDelim;
    @StoredAsProperty("field.fieldTitle")
    private String[] fieldTitle;

    @StoredAsProperty("file.urls")
    private String[] filePaths;
    @StoredAsProperty("field.delims")
    private String[] fieldDelims;



    @StoredAsProperty("db.url")
    private String url;
    @StoredAsProperty("db.table")
    private String tableName;
    @StoredAsProperty("db.username")
    private String username;
    @StoredAsProperty("db.password")
    private String password;
    @StoredAsProperty("db.driver.class")
    private String driverClass;
    @StoredAsProperty("db.sql")
    private String dbSQL;
    @StoredAsProperty("db.column")
    private String[] dbColumn;

    @StoredAsProperty("db.driver.class")
    private String[] driverClasss;
    @StoredAsProperty("db.urls")
    private String[] urls;
    @StoredAsProperty("db.tables")
    private String[] tableNames;
    @StoredAsProperty("db.usernames")
    private String[] usernames;
    @StoredAsProperty("db.passwords")
    private String[] passwords;


    @StoredAsProperty("es.addrs")
    private String[] esAddrs;
    @StoredAsProperty("es.index")
    private String esIndex;
    @StoredAsProperty("es.type")
    private String esType;
    @StoredAsProperty("es.query")
    private String esQuery;
    @StoredAsProperty("es.idFn")
    private String esIdFn;

    @StoredAsProperty("es.addrs.from")
    private String[] esAddrsFrom;
    @StoredAsProperty("es.addrs.to")
    private String[] esAddrsTo;

    @StoredAsProperty("es.indexs")
    private String[] esIndexs;
    @StoredAsProperty("es.type")
    private String[] esTypes;



    @StoredAsProperty("hive.driver.class")
    private String hiveClass;

    private String hiveUrl;
    @StoredAsProperty("hive.username")
    private String hiveUsername;
    @StoredAsProperty("hive.password")
    private String hivePassword;
    @StoredAsProperty("hive.table")
    private String hiveTableName;
    @StoredAsProperty("hive.database")
    private String hiveDatabase;
    @StoredAsProperty("hive.table.empty")
    private Boolean hiveTableEmpty;


    @StoredAsProperty("hive.sql")
    private String hiveSQL;
    @StoredAsProperty("hive.column")
    private String[] hiveColumn;
    @StoredAsProperty("hcatalog.metastore")
    private String hMetastoreHost;
    @StoredAsProperty("hcatalog.metastore.port")
    private String hMetastorePort;
    @StoredAsProperty("hcatalog.metastore.filter")
    private String hiveFilter;
    @StoredAsProperty("hive.metastore.partition")
    private String hivePartition;

    @StoredAsProperty("hive.urls")
    private String[] hiveUrls;
    @StoredAsProperty("hive.usernames")
    private String[] hiveUsernames;
    @StoredAsProperty("hive.passwords")
    private String[] hivePasswords;
    @StoredAsProperty("hive.tables")
    private String[] hiveTableNames;
    @StoredAsProperty("hive.databases")
    private String[] hiveDatabases;


    @StoredAsProperty("hcatalog.metastores")
    private String[] hMetastoreHosts;
    @StoredAsProperty("hcatalog.metastore.ports")
    private String[] hMetastorePorts;



    @StoredAsProperty("neo4j.cypher")
    private String cypher;
    @StoredAsProperty("neo4j.neourl")
    private String neoUrl;
    @StoredAsProperty("neo4j.neousername")
    private String neoUsername;
    @StoredAsProperty("neo4j.neopassword")
    private String neoPassword;
    @StoredAsProperty("neo4j.neoformat")
    private String neoFormat;
    @StoredAsProperty("neo4j.label")
    private String neoLabel;


    /**
     * 嵌套 存储es
     */
    @StoredAsProperty("db.嵌套keys")
    private String[] nestingKeys;
    @StoredAsProperty("db.普通列")
    private String[] nestingColumns;
    @StoredAsProperty("db.嵌套列")
    private String[] nestingValues;



    private NestingFields nestingFields;

    private void putProperty(Properties props, String k, String v) {
        if (null == v) {
            props.remove(k);
        } else {
            props.setProperty(k, v);
        }
    }

    private String arrayToList(String [] array) {
        if (null == array) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String elem : array) {
            if (!first) {
                sb.append(",");
            }
            sb.append(elem);
            first = false;
        }

        return sb.toString();
    }


    public void loadProperties(Properties props) {

        try {
            Field[] fields = SwapOptions.class.getDeclaredFields();
            for (Field f : fields) {
                if (f.isAnnotationPresent(StoredAsProperty.class)) {
                    Class typ = f.getType();
                    StoredAsProperty storedAs = f.getAnnotation(StoredAsProperty.class);
                    String propName = storedAs.value();

                    if (typ.equals(int.class)) {
                        f.setInt(this,
                                getIntProperty(props, propName, f.getInt(this)));
                    } else if (typ.equals(boolean.class)) {
                        f.setBoolean(this,
                                getBooleanProperty(props, propName, f.getBoolean(this)));
                    } else if (typ.equals(long.class)) {
                        f.setLong(this,
                                getLongProperty(props, propName, f.getLong(this)));
                    } else if (typ.equals(String.class)) {
                        f.set(this, props.getProperty(propName, (String) f.get(this)));
                    } else if (typ.equals(Integer.class)) {
                        String value = props.getProperty(
                                propName,
                                f.get(this) == null ? "null" : f.get(this).toString());
                        f.set(this, value.equals("null") ? null : new Integer(value));
                    } else if (typ.isEnum()) {
                        f.set(this, Enum.valueOf(typ,
                                props.getProperty(propName, f.get(this).toString())));
                    } else if (typ.equals(Map.class)) {
                        f.set(this,
                                BaseUtil.JsonUtils.jsonObjectToObject(props.getProperty(propName),Map.class));
                    } else {
                        throw new RuntimeException("Could not retrieve property "
                                + propName + " for type: " + typ);
                    }
                }
            }
        } catch (IllegalAccessException iae) {
            throw new RuntimeException("Illegal access to field in property setter",
                    iae);
        }
    }



    private boolean getBooleanProperty(Properties props, String propName,
                                       boolean defaultValue) {
        String str = props.getProperty(propName,
                Boolean.toString(defaultValue)).toLowerCase();
        return "true".equals(str) || "yes".equals(str) || "1".equals(str);
    }

    private long getLongProperty(Properties props, String propName,
                                 long defaultValue) {
        String str = props.getProperty(propName,
                Long.toString(defaultValue)).toLowerCase();
        try {
            return Long.parseLong(str);
        } catch (NumberFormatException nfe) {
            logger.warn("Could not parse integer value for config parameter "
                    + propName);
            return defaultValue;
        }
    }

    private int getIntProperty(Properties props, String propName,
                               int defaultVal) {
        long longVal = getLongProperty(props, propName, defaultVal);
        return (int) longVal;
    }

    private char getCharProperty(Properties props, String propName,
                                 char defaultVal) {
        int intVal = getIntProperty(props, propName, (int) defaultVal);
        return (char) intVal;
    }



    private String [] listToArray(String strList) {
        return strList.split("\\,");
    }

    public Properties writeProperties() {
        Properties props = new Properties();

        try {
            Field [] fields = SwapOptions.class.getDeclaredFields();
            for (Field f : fields) {
                if (f.isAnnotationPresent(StoredAsProperty.class)) {
                    Class typ = f.getType();
                    StoredAsProperty storedAs = f.getAnnotation(StoredAsProperty.class);
                    String propName = storedAs.value();

                    if (typ.equals(int.class)) {
                        putProperty(props, propName, Integer.toString(f.getInt(this)));
                    } else if (typ.equals(boolean.class)) {
                        putProperty(props, propName, Boolean.toString(f.getBoolean(this)));
                    } else if (typ.equals(long.class)) {
                        putProperty(props, propName, Long.toString(f.getLong(this)));
                    } else if (typ.equals(String.class)) {
                        putProperty(props, propName, (String) f.get(this));
                    } else if (typ.equals(Integer.class)) {
                        putProperty(
                                props,
                                propName,
                                f.get(this) == null ? "null" : f.get(this).toString());
                    } else if (typ.isEnum()) {
                        putProperty(props, propName, f.get(this).toString());
                    } else if (typ.equals(Map.class)) {
                        Map map = (Map) f.get(this);
                        String jsonString = JSON.toJSONString(map);
                        putProperty(
                                props,
                                propName,
                                jsonString);

                    } else {
                        throw new RuntimeException("Could not set property "
                                + propName + " for type: " + typ);
                    }
                }
            }
        } catch (IllegalAccessException iae) {
            throw new RuntimeException("Illegal access to field in property setter",
                    iae);
        }

        writePasswordProperty(props);

        putProperty(props, "db.column", arrayToList(this.dbColumn));
        putProperty(props, "hive.column", arrayToList(this.hiveColumn));

        return props;
    }

    private void writePasswordProperty(Properties props) {

    }

    /**
     * 嵌套字段
     *  keys 根据keys合并，名称
     *  columns 普通字段名称
     *  nestings 合并字段名称 key 嵌套字段名称，string[] 合并字段名称
     * @return 返回值
     */
    private void setNestingFields(){
        if(this.nestingFields == null && nestingKeys != null  && nestingValues!=null){
            NestingFields nestingFields = new NestingFields();
            nestingFields.setColumns(nestingColumns);
            Map<String,String[]> nestings=new HashMap<>();
            //nestings key为结果表嵌套字段名称
            nestings.put("nestingsjson",nestingValues);
            nestingFields.setNestings(nestings);
            nestingFields.setKeys(nestingKeys);
            this.nestingFields = nestingFields;
        }
    }

    public NestingFields getNestingFields() {
        setNestingFields();
        return nestingFields;
    }

    public void setNestingFields(NestingFields nestingFields) {
        this.nestingFields = nestingFields;
    }
}
