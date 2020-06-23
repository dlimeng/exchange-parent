package com.knowlegene.parent.process.pojo.db;

import com.knowlegene.parent.process.common.annotation.StoredAsProperty;
import lombok.Data;

import java.io.Serializable;

/**
 * @Classname DBOptions
 * @Description TODO
 * @Date 2020/6/22 10:41
 * @Created by limeng
 */
@Data
public class DBOptions implements Serializable {
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
}
