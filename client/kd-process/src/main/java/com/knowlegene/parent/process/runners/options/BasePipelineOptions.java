package com.knowlegene.parent.process.runners.options;

import org.apache.beam.sdk.options.Description;

import java.io.Serializable;

/**
 * @Classname BasePipelineOptions
 * @Description TODO
 * @Date 2020/7/3 15:50
 * @Created by limeng
 */
public interface BasePipelineOptions extends Serializable {
    @Description("导出名称")
    public String getFromName();
    public void setFromName(String fromName);
    @Description("导入名称")
    public String getToName();
    public void setToName(String toName);


    @Description("文件路径")
    public String getFilePath();
    public void setFilePath(String filePath);
    @Description("文件字段分割符")
    public String getFieldDelim();
    public void setFieldDelim(String fieldDelim);

    @Description("文件标题")
    public String[] getFieldTitle();
    public void setFieldTitle(String fieldTitle);

    @Description("文件路径")
    public String[] getFilePaths();
    public void setFilePaths(String filePaths);

    @Description("文件字段分割符")
    public String[] getFieldDelims();
    public void setFieldDelims(String fieldDelims);


    @Description("db.url")
    public String getUrl();
    public void setUrl(String url);
    @Description("db.tablename")
    public String getTableName();
    public void setTableName(String tableName);
    @Description("db.username")
    public String getUsername();
    public void setUsername(String username);
    @Description("db.password")
    public String getPassword();
    public void setPassword(String password);
    @Description("db.driverClass")
    public String getDriverClass();
    public void setDriverClass(String driverClassName);
    @Description("db.dbSQL")
    public String getDbSQL();
    public void setDbSQL(String dbSQL);


    @Description("db.dbColumn")
    public String[] getDbColumn();
    public void setDbColumn(String[] dbColumn);


    @Description("db.driver.class")
    public String[] getDriverClasss();
    public void setDriverClasss(String[] driverClasss);

    @Description("db.urls")
    public String[] getUrls();
    public void setUrls(String[] urls);

    @Description("db.tables")
    public String[] getTableNames();
    public void setTableNames(String[] tableNames);

    @Description("db.usernames")
    public String[] getUsernames();
    public void setUsernames(String[] usernames);

    @Description("db.passwords")
    public String[] getPasswords();
    public void setPasswords(String[] passwords);



    @Description("db.hiveClass")
    public String getHiveClass();
    public void setHiveClass(String hiveClass);


    @Description("db.hive.execution.engine")
    public String getHiveEngine();
    public void setHiveEngine(String hiveEngine);

    @Description("db.hiveUrl")
    public String getHiveUrl() ;
    public void setHiveUrl(String hiveUrl);

    @Description("db.hiveUsername")
    public String getHiveUsername();
    public void setHiveUsername(String hiveUsername);
    @Description("db.hivePassword")
    public String getHivePassword();
    public void setHivePassword(String hivePassword);
    @Description("db.hiveTableName")
    public String getHiveTableName();
    public void setHiveTableName(String hiveTableName);

    @Description("db.hivedatabase")
    public String getHiveDatabase();
    public void setHiveDatabase(String hiveDatabase);

    @Description("db.hiveSQL")
    public String getHiveSQL() ;
    public void setHiveSQL(String hiveSQL);
    @Description("db.hiveColumn")
    public String[] getHiveColumn();
    public void setHiveColumn(String[] hiveColumn);


    @Description("db.hMetastoreHost")
    public String getHMetastoreHost();
    public void setHMetastoreHost(String hCatalogMetastoreHostName);

    @Description("db.hMetastorePort")
    public String getHMetastorePort();
    public void setHMetastorePort(String hCatalogMetastorePort) ;

    @Description("db.hMetastore.hivePartition")
    public String getHivePartition();
    public void setHivePartition(String hivePartition);

    @Description("db.hMetastore.hiveFilter")
    public String getHiveFilter();
    public void setHiveFilter(String hiveFilter);

    @Description("hive表是否清空")
    public Boolean getHiveTableEmpty();
    public void setHiveTableEmpty(Boolean hiveTableEmpty);


    @Description("db.hMetastoreHosts")
    public String[] getHMetastoreHosts();
    public void setHMetastoreHosts(String[] hCatalogMetastoreHostNames);

    @Description("db.hMetastorePorts")
    public String[] getHMetastorePorts();
    public void setHMetastorePorts(String[] hCatalogMetastorePorts) ;



    @Description("db.hiveUrls")
    public String[] getHiveUrls() ;
    public void setHiveUrls(String[] hiveUrls);

    @Description("db.hiveUsernames")
    public String[] getHiveUsernames();
    public void setHiveUsernames(String[] hiveUsernames);
    @Description("db.hivePasswords")
    public String[] getHivePasswords();
    public void setHivePasswords(String[] hivePasswords);

    @Description("db.hiveTableNames")
    public String[] getHiveTableNames();
    public void setHiveTableNames(String[] hiveTableNames);


    @Description("db.hivedatabases")
    public String[] getHiveDatabases();
    public void setHiveDatabases(String[] hiveDatabases);


    @Description("es 地址")
    public String[] getEsAddrs();
    public void setEsAddrs(String[] esAddrs);

    @Description("es index")
    public String getEsIndex();
    public void setEsIndex(String esIndex);

    @Description("es type")
    public String getEsType();
    public void setEsType(String esType);

    @Description("es query")
    public String getEsQuery();
    public void setEsQuery(String esQuery);

    @Description("es idFn")
    public String getEsIdFn();
    public void setEsIdFn(String esIdFn);


    @Description("es.addrs.from")
    public String[] getEsAddrsFrom();
    public void setEsAddrsFrom(String[] esAddrsFrom);

    @Description("es.addrs.to")
    public String[] getEsAddrsTo();
    public void setEsAddrsTo(String[] esAddrsTo);

    @Description("es.indexs")
    public String[] getEsIndexs();
    public void setEsIndexs(String[] esIndexs);

    @Description("es.type")
    public String[] getEsTypes();
    public void setEsTypes(String[] esTypes);

    @Description("neo4j.cypher")
    String getCypher();
    void  setCypher(String cypher);


    @Description("neo4j.neourl")
    String getNeoUrl();
    void setNeoUrl(String neoUrl);

    @Description("neo4j.neousername")
    String getNeoUsername();
    void setNeoUsername(String neoUsername);

    @Description("neo4j.neopassword")
    String getNeoPassword();
    void setNeoPassword(String neoPassword);

    @Description("neo4j.neoformat")
    String getNeoFormat();
    void setNeoFormat(String neoFormat);

    @Description("neo4j.neoLabel")
    String getNeoLabel();
    void  setNeoLabel(String neoLabel);


    @Description("neo4j.type")
    public String getNeoType();
    public void setNeoType(String neoType);

    @Description("neo4j.cyphers")
    public String[] getCyphers();
    public void setCyphers(String[] cyphers);

    @Description("neo4j.neourls")
    public String[] getNeoUrls();
    public void setNeoUrls(String[] neoUrls);

    @Description("neo4j.neousernames")
    public String[] getNeoUsernames();
    public void setNeoUsernames(String[] neoUsernames);


    @Description("neo4j.neopasswords")
    public String[] getNeoPasswords();
    public void setNeoPasswords(String[] neoPasswords);
}
