package com.knowlegene.parent.process.pojo.hive;

import com.knowlegene.parent.process.common.annotation.StoredAsProperty;
import lombok.Data;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * @Classname HiveOptions
 * @Description TODO
 * @Date 2020/6/11 16:48
 * @Created by limeng
 */
@Data
public class HiveOptions implements Serializable {
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



    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HiveOptions that = (HiveOptions) o;
        return Objects.equals(hiveClass, that.hiveClass) &&
                Objects.equals(hiveUrl, that.hiveUrl) &&
                Objects.equals(hiveUsername, that.hiveUsername) &&
                Objects.equals(hivePassword, that.hivePassword) &&
                Objects.equals(hiveTableName, that.hiveTableName) &&
                Objects.equals(hiveDatabase, that.hiveDatabase) &&
                Objects.equals(hiveTableEmpty, that.hiveTableEmpty) &&
                Objects.equals(hiveSQL, that.hiveSQL) &&
                Arrays.equals(hiveColumn, that.hiveColumn) &&
                Objects.equals(hMetastoreHost, that.hMetastoreHost) &&
                Objects.equals(hMetastorePort, that.hMetastorePort) &&
                Objects.equals(hiveFilter, that.hiveFilter) &&
                Objects.equals(hivePartition, that.hivePartition) ;
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(hiveClass, hiveUrl, hiveUsername, hivePassword, hiveTableName, hiveDatabase, hiveTableEmpty, hiveSQL, hMetastoreHost, hMetastorePort, hiveFilter, hivePartition);
        result = 31 * result + Arrays.hashCode(hiveColumn);
        return result;
    }
}
