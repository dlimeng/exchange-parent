package com.knowlegene.parent.process.util;

import com.knowlegene.parent.config.common.constantenum.DBOperationEnum;
import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.pojo.DataSources;
import com.knowlegene.parent.process.pojo.NestingFields;
import com.knowlegene.parent.process.pojo.SwapOptions;
import com.knowlegene.parent.process.pojo.db.DBOptions;
import com.knowlegene.parent.process.pojo.es.ESOptions;
import com.knowlegene.parent.process.pojo.file.FileOptions;
import com.knowlegene.parent.process.pojo.hive.HiveOptions;
import com.knowlegene.parent.process.pojo.neo4j.Neo4jOptions;
import com.knowlegene.parent.scheduler.utils.CacheManager;

import javax.sql.DataSource;

/**
 * @Classname DataSourceUtil
 * @Description TODO
 * @Date 2020/6/10 14:12
 * @Created by limeng
 */
public class DataSourceUtil {

    /**
     * 设置连接池
     * @param keys
     * @param dataSources
     */
    private static void setDataSourcePool(String keys,DataSources dataSources) {

        if(BaseUtil.isBlank(keys)) return;

        String driverClassName = dataSources.getClassName();
        String username = dataSources.getUser();
        String password = dataSources.getPassword();
        String usrl = dataSources.getUrl();
        DataSource dataSource = ComboPooledDataSourceUtil.setDataSource(driverClassName, username, password, usrl);
        if(dataSource == null) return;
        CacheManager.setCache(keys,dataSource);
    }

    /**
     * hive 操作实体
     * @param keys
     * @param options
     */
    public static void setHive(String keys, SwapOptions options){
        if(options == null) return;
        if(BaseUtil.isBlank(keys)) return;
        HiveOptions hiveOptions = new HiveOptions();
        BaseUtil.copyNonNullProperties(hiveOptions,options);
        if(hiveOptions == null) return;

        String dbKeys = getDBName(keys);
        if(BaseUtil.isBlank(dbKeys)) return;

        setDataSourcePool(dbKeys,DataSources.builder()
                .className(hiveOptions.getHiveClass())
                .url(hiveOptions.getHiveUrl())
                .user(hiveOptions.getHiveUsername())
                .password(hiveOptions.getHivePassword())
                .build());



        CacheManager.setCache(keys,hiveOptions);
    }


    public static void setHiveImExport(String keys, SwapOptions options){
        if(options == null) return;
        if(BaseUtil.isBlank(keys)) return;
        String[] hiveDatabases = options.getHiveDatabases();

        String[] hiveUrls = options.getHiveUrls();

        String[] hiveUsernames = options.getHiveUsernames();
        String[] hivePasswords = options.getHivePasswords();
        String[] hiveTableNames = options.getHiveTableNames();
        String[] hMetastoreHosts = options.getHMetastoreHosts();
        String[] hMetastorePorts = options.getHMetastorePorts();
        // exort 1  import 2

        int hiveUrlLength = hiveUrls == null ? 0:hiveUrls.length;
        int hiveDataLength = hiveDatabases == null ? 0 : hiveDatabases.length;
        int hiveUserLength = hiveUsernames == null ? 0 : hiveUsernames.length;
        int hivePwLength = hivePasswords == null ? 0 : hivePasswords.length;
        int hiveTableLength =hiveTableNames == null ? 0 : hiveTableNames.length;
        int hMHostLength = hMetastoreHosts == null ? 0 : hMetastoreHosts.length;
        int hMPortslength = hMetastorePorts == null ? 0 : hMetastorePorts.length;

        HiveOptions hiveOptions = new HiveOptions();
        BaseUtil.copyProperties(hiveOptions,options);

        int exIndex = 0;
        int imIndex = 1;

        String dbKeys="";

        if(keys.equals(DBOperationEnum.HIVE_EXPORT.getName())){
            if(hiveUrlLength > exIndex) hiveOptions.setHiveUrl(hiveUrls[exIndex]);

            if(hiveDataLength > exIndex) hiveOptions.setHiveDatabase(hiveDatabases[exIndex]);
            if(hiveUserLength > exIndex) hiveOptions.setHiveUsername(hiveUsernames[exIndex]);
            if(hivePwLength > exIndex) hiveOptions.setHivePassword(hivePasswords[exIndex]);

            if(hiveTableLength > exIndex) hiveOptions.setHiveTableName(hiveTableNames[exIndex]);
            if(hMHostLength > exIndex) hiveOptions.setHMetastoreHost(hMetastoreHosts[exIndex]);
            if(hMPortslength > exIndex) hiveOptions.setHMetastorePort(hMetastorePorts[exIndex]);

            dbKeys = DBOperationEnum.HIVE_EXPORT_DB.getName();

        }else if(keys.equals(DBOperationEnum.HIVE_IMPORT.getName())){
            if(hiveUrlLength > imIndex) hiveOptions.setHiveUrl(hiveUrls[imIndex]);

            if(hiveDataLength > imIndex) hiveOptions.setHiveDatabase(hiveDatabases[imIndex]);
            if(hiveUserLength > imIndex) hiveOptions.setHiveUsername(hiveUsernames[imIndex]);
            if(hivePwLength > imIndex) hiveOptions.setHivePassword(hivePasswords[imIndex]);

            if(hiveTableLength > imIndex) hiveOptions.setHiveTableName(hiveTableNames[imIndex]);
            if(hMHostLength > imIndex) hiveOptions.setHMetastoreHost(hMetastoreHosts[imIndex]);
            if(hMPortslength > imIndex) hiveOptions.setHMetastorePort(hMetastorePorts[imIndex]);

            dbKeys = DBOperationEnum.HIVE_IMPORT_DB.getName();

        }


        if(hiveOptions == null) return;



        setDataSourcePool(dbKeys,DataSources.builder()
                .className(hiveOptions.getHiveClass())
                .url(hiveOptions.getHiveUrl())
                .user(hiveOptions.getHiveUsername())
                .password(hiveOptions.getHivePassword())
                .build());



        CacheManager.setCache(keys,hiveOptions);
    }


    public static void setDBImExport(String keys, SwapOptions options){
        if(options == null) return;
        if(BaseUtil.isBlank(keys)) return;

        String[] driverClasss = options.getDriverClasss();

        String[] urls = options.getUrls();
        String[] tableNames = options.getTableNames();
        String[] usernames = options.getUsernames();
        String[] passwords = options.getPasswords();


        // exort 1  import 2

        int driverLength = driverClasss == null ? 0:driverClasss.length;
        int urlLength = urls == null ? 0:urls.length;
        int userLength = usernames == null ? 0 : usernames.length;
        int pwLength = passwords == null ? 0 : passwords.length;
        int tableLength =tableNames == null ? 0 : tableNames.length;


        DBOptions dboptions = new DBOptions();
        BaseUtil.copyProperties(dboptions,options);

        int exIndex = 0;
        int imIndex = 1;



        String dbKeys = getDBName(keys);

        if(keys.contains(DBOperationEnum.EXPORT.getName())){
            if(urlLength > exIndex) dboptions.setUrl(urls[exIndex]);

            if(userLength > exIndex) dboptions.setUsername(usernames[exIndex]);
            if(pwLength > exIndex) dboptions.setPassword(passwords[exIndex]);

            if(tableLength > exIndex) dboptions.setTableName(tableNames[exIndex]);

            if(driverLength > exIndex) dboptions.setDriverClass(driverClasss[exIndex]);


        }else if(keys.contains(DBOperationEnum.IMPORT.getName())){
            if(urlLength > imIndex) dboptions.setUrl(urls[imIndex]);

            if(userLength > imIndex) dboptions.setUsername(usernames[imIndex]);
            if(pwLength > imIndex) dboptions.setPassword(passwords[imIndex]);

            if(tableLength > imIndex) dboptions.setTableName(tableNames[imIndex]);

            if(driverLength > imIndex) dboptions.setDriverClass(driverClasss[imIndex]);

        }

        if(BaseUtil.isBlank(dbKeys)) return;
        if(dboptions == null) return;



        setDataSourcePool(dbKeys,DataSources.builder()
                .className(dboptions.getDriverClass())
                .url(dboptions.getUrl())
                .user(dboptions.getUsername())
                .password(dboptions.getPassword())
                .build());

        CacheManager.setCache(keys,dboptions);

    }


    /**setDb
     * oracle gbase mysql
     * @param keys
     * @param options
     */
    public static void setDb(String keys, SwapOptions options){
        if(options == null) return;
        if(BaseUtil.isBlank(keys)) return;


        DBOptions dboptions = new DBOptions();
        BaseUtil.copyProperties(dboptions,options);
        if(dboptions == null) return;



        String dbKeys = getDBName(keys);
        if(BaseUtil.isBlank(dbKeys)) return;


        setDataSourcePool(dbKeys,DataSources.builder()
                .className(dboptions.getDriverClass())
                .url(dboptions.getUrl())
                .user(dboptions.getUsername())
                .password(dboptions.getPassword())
                .build());


        CacheManager.setCache(keys,dboptions);

    }


    public static void setEsImExport(String keys, SwapOptions options){
        if(options == null) return;

        String[] esAddrsFrom = options.getEsAddrsFrom();
        String[] esAddrsTo = options.getEsAddrsTo();
        String[] esIndexs = options.getEsIndexs();
        String[] esTypes = options.getEsTypes();


        ESOptions esoptions = new ESOptions();
        BaseUtil.copyProperties(esoptions,options);


        // exort 1  import 2
        int esFromLength = esAddrsFrom == null ? 0:esAddrsFrom.length;
        int esToLength = esAddrsTo == null ? 0:esAddrsTo.length;

        int esIndexLength = esIndexs == null ? 0:esIndexs.length;
        int esTypeLength = esTypes == null ? 0:esTypes.length;


        int exIndex = 0;
        int imIndex = 1;


        if(keys.contains(DBOperationEnum.EXPORT.getName())){
            if(esFromLength > exIndex) esoptions.setEsAddrs(esAddrsFrom);

            if(esIndexLength > exIndex) esoptions.setEsIndex(esIndexs[exIndex]);
            if(esTypeLength > exIndex) esoptions.setEsType(esIndexs[exIndex]);


        }else if(keys.contains(DBOperationEnum.IMPORT.getName())){
            if(esToLength > exIndex) esoptions.setEsAddrs(esAddrsTo);

            if(esIndexLength > imIndex) esoptions.setEsIndex(esIndexs[imIndex]);
            if(esTypeLength > imIndex) esoptions.setEsType(esIndexs[imIndex]);
        }


        if(esoptions == null) return;

        NestingFields nestingFields = options.getNestingFields();
        if(nestingFields != null){
            esoptions.setNestingFields(nestingFields);
        }

        CacheManager.setCache(keys,esoptions);
    }



    public static void setEs(String keys, SwapOptions options){
        if(options == null) return;

        ESOptions esoptions = new ESOptions();
        BaseUtil.copyProperties(esoptions,options);

        if(esoptions == null) return;

        NestingFields nestingFields = options.getNestingFields();
        if(nestingFields != null){
            esoptions.setNestingFields(nestingFields);
        }

        CacheManager.setCache(keys,esoptions);

    }


    public static void setFileImExport(String keys, SwapOptions options){
        if(options == null) return;

        String[] fieldDelims = options.getFieldDelims();
        String[] filePaths = options.getFilePaths();



        FileOptions fileoptions = new FileOptions();
        BaseUtil.copyProperties(fileoptions,options);


        // exort 1  import 2
        int fieldDelimLength = fieldDelims == null ? 0:fieldDelims.length;
        int filePathLength = filePaths == null ? 0:filePaths.length;



        int exIndex = 0;
        int imIndex = 1;


        if(keys.contains(DBOperationEnum.EXPORT.getName())){
            if(fieldDelimLength > exIndex) fileoptions.setFieldDelim(fieldDelims[exIndex]);

            if(filePathLength > exIndex) fileoptions.setFilePath(filePaths[exIndex]);


        }else if(keys.contains(DBOperationEnum.IMPORT.getName())){
            if(fieldDelimLength > imIndex) fileoptions.setFieldDelim(fieldDelims[imIndex]);

            if(filePathLength > imIndex)  fileoptions.setFilePath(filePaths[imIndex]);

        }


        if(fileoptions == null) return;


        CacheManager.setCache(keys,fileoptions);
    }

    public static void setFile(String keys, SwapOptions options){
        if(options == null) return;

        FileOptions fileoptions = new FileOptions();
        BaseUtil.copyProperties(fileoptions,options);

        if(fileoptions == null) return;

        CacheManager.setCache(keys,fileoptions);
    }


    public static void setNeo4jImExport(String keys, SwapOptions options){
        if(options == null) return;

        String[] cyphers = options.getCyphers();
        String[] neoUrls = options.getNeoUrls();
        String[] neoUsernames = options.getNeoUsernames();
        String[] neoPasswords = options.getNeoPasswords();

        // exort 1  import 2
        int cyphersLength = cyphers == null ? 0:cyphers.length;
        int urlLength = neoUrls == null ? 0:neoUrls.length;
        int userLength = neoUsernames == null ? 0:neoUsernames.length;
        int passwordsLength = neoPasswords == null ? 0:neoPasswords.length;


        Neo4jOptions noptions = new Neo4jOptions();
        BaseUtil.copyProperties(noptions,options);


        int exIndex = 0;
        int imIndex = 1;

        if(keys.contains(DBOperationEnum.EXPORT.getName())){
            if(cyphersLength > exIndex) noptions.setCypher(cyphers[exIndex]);

            if(urlLength > exIndex) noptions.setNeoUrl(neoUrls[exIndex]);

            if(userLength > exIndex) noptions.setNeoUsername(neoUsernames[exIndex]);

            if(passwordsLength > exIndex) noptions.setNeoPassword(neoPasswords[exIndex]);


        }else if(keys.contains(DBOperationEnum.IMPORT.getName())){
            if(cyphersLength > imIndex) noptions.setCypher(cyphers[imIndex]);

            if(urlLength > imIndex) noptions.setNeoUrl(neoUrls[imIndex]);

            if(userLength > imIndex) noptions.setNeoUsername(neoUsernames[imIndex]);

            if(passwordsLength > imIndex) noptions.setNeoPassword(neoPasswords[imIndex]);
        }


        if(noptions == null) return;

        CacheManager.setCache(keys,noptions);
    }

    public static void setNeo4j(String keys, SwapOptions options){
        if(options == null) return;


        Neo4jOptions noptions = new Neo4jOptions();
        BaseUtil.copyProperties(noptions,options);
        if(noptions == null) return;

        CacheManager.setCache(keys,noptions);
    }


    private static String getDBName(String keys){
        DBOperationEnum anEnum = DBOperationEnum.getEnum(keys);
        String result="";
        switch (anEnum){
            case MYSQL_EXPORT:
                result = DBOperationEnum.MYSQL_EXPORT_DB.getName();
                break;
            case MYSQL_IMPORT:
                result = DBOperationEnum.MYSQL_IMPORT_DB.getName();
                break;
            case ORACLE_EXPORT:
                result = DBOperationEnum.ORACLE_EXPORT_DB.getName();
                break;
            case ORACLE_IMPORT:
                result = DBOperationEnum.ORACLE_IMPORT_DB.getName();
                break;
            case GBASE_EXPORT:
                result = DBOperationEnum.GBASE_EXPORT_DB.getName();
                break;
            case GBASE_IMPORT:
                result = DBOperationEnum.GBASE_IMPORT_DB.getName();
                break;
            case HIVE_EXPORT:
                result =  DBOperationEnum.HIVE_EXPORT_DB.getName();
                break;
            case HIVE_IMPORT:
                result =  DBOperationEnum.HIVE_IMPORT_DB.getName();
                break;

        }
        return result;
    }

}
