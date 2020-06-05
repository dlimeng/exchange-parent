package com.knowlegene.parent.config.dao.impl;

import com.knowlegene.parent.config.dao.BaseDao;
import com.knowlegene.parent.config.dao.HiveDao;
import com.knowlegene.parent.config.dao.MysqlDao;

import javax.annotation.Resource;
import java.io.Serializable;

/**
 * @Author: limeng
 * @Date: 2019/7/15 17:10
 */
public class BaseDaoImpl implements BaseDao, Serializable {

    private static final long serialVersionUID = -3394748707344918532L;

    private HiveDao hiveDao=new HiveDaoImpl();

    private MysqlDao mysqlDao = new MysqlDaoImpl();

    public HiveDao getHiveDao() {
        return hiveDao;
    }

    public MysqlDao getMysqlDao() {
        return mysqlDao;
    }
}
