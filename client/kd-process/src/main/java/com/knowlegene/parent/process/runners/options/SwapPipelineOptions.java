package com.knowlegene.parent.process.runners.options;

import com.knowlegene.parent.process.common.annotation.StoredAsProperty;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.sdk.options.Description;

import java.util.List;

/**
 * @Author: limeng
 * @Date: 2019/8/22 16:51
 */
public interface SwapPipelineOptions extends SparkPipelineOptions {
    @Description("文件路径")
    public String getFilePath();
    public void setFilePath(String filePath);
    @Description("文件字段分割符")
    public String getFieldDelim();
    public void setFieldDelim(String fieldDelim);

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
    public List<String> getDbColumn();
    public void setDbColumn(List<String> dbColumn);

    @Description("db.hiveClass")
    public Boolean getHiveClass();
    public void setHiveClass(Boolean hiveClass);

    @Description("db.hiveImport")
    public Boolean getHiveImport();
    public void setHiveImport(Boolean hiveImport);

    @Description("db.hiveExport")
    public Boolean getHiveExport();
    public void setHiveExport(Boolean hiveExport);

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
    @Description("db.hiveSQL")
    public String getHiveSQL() ;
    public void setHiveSQL(String hiveSQL);
    @Description("db.hiveColumn")
    public List<String> getHiveColumn();
    public void setHiveColumn(List<String> hiveColumn) ;

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

    @Description("操作链路顺序 导入")
    public Boolean getImportOptions();
    public void setImportOptions(Boolean importOptions);

    @Description("操作链路顺序 导出")
    public Boolean getExportOptions();
    public void setExportOptions(Boolean exportOptions);

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
}
