package com.knowlegene.parent.process.swap.common;


import com.knowlegene.parent.process.extract.ExtractFile;
import com.knowlegene.parent.process.extract.ExtractHive;
import com.knowlegene.parent.process.io.es.ESSwap;
import com.knowlegene.parent.process.io.es.impl.ESSwapImpl;
import com.knowlegene.parent.process.io.jdbc.GbaseSwap;
import com.knowlegene.parent.process.io.jdbc.HiveSwap;
import com.knowlegene.parent.process.io.jdbc.MySQLSwap;
import com.knowlegene.parent.process.io.jdbc.OracleSwap;
import com.knowlegene.parent.process.io.jdbc.impl.GbaseSwapImpl;
import com.knowlegene.parent.process.io.jdbc.impl.HiveSwapImpl;
import com.knowlegene.parent.process.io.jdbc.impl.MySQLSwapImpl;
import com.knowlegene.parent.process.io.jdbc.impl.OracleSwapImpl;
import com.knowlegene.parent.process.io.neo4j.Neo4jSwap;
import com.knowlegene.parent.process.io.neo4j.impl.Neo4jSwapImpl;


import javax.annotation.Resource;
import java.io.Serializable;

/**
 * 公共交换方法
 * @Author: limeng
 * @Date: 2019/8/26 15:17
 */
public class BaseSwap  implements Serializable {
    @Resource
    private HiveSwap hiveSwap;
    @Resource
    private MySQLSwap mySQLSwap;
    @Resource
    private OracleSwap oracleSwap;
    @Resource
    private ESSwap eSSwap;
    @Resource
    private Neo4jSwap neo4jSwap;
    @Resource
    private GbaseSwap gbaseSwap;

    public BaseSwap() {
        this.hiveSwap = new HiveSwapImpl();
        this.mySQLSwap = new MySQLSwapImpl();
        this.oracleSwap = new OracleSwapImpl();
        this.eSSwap = new ESSwapImpl();
        this.neo4jSwap = new Neo4jSwapImpl();
        this.gbaseSwap = new GbaseSwapImpl();
    }

    public HiveSwap getHiveSwap() {
        return hiveSwap;
    }

    public void setHiveSwap(HiveSwap hiveSwap) {
        this.hiveSwap = hiveSwap;
    }

    public MySQLSwap getMySQLSwap() {
        return mySQLSwap;
    }

    public void setMySQLSwap(MySQLSwap mySQLSwap) {
        this.mySQLSwap = mySQLSwap;
    }

    public OracleSwap getOracleSwap() {
        return oracleSwap;
    }

    public void setOracleSwap(OracleSwap oracleSwap) {
        this.oracleSwap = oracleSwap;
    }

    public ESSwap geteSSwap() {
        return eSSwap;
    }

    public void seteSSwap(ESSwap eSSwap) {
        this.eSSwap = eSSwap;
    }

    public Neo4jSwap getNeo4jSwap() {
        return neo4jSwap;
    }

    public void setNeo4jSwap(Neo4jSwap neo4jSwap) {
        this.neo4jSwap = neo4jSwap;
    }

    public GbaseSwap getGbaseSwap() {
        return gbaseSwap;
    }

    public void setGbaseSwap(GbaseSwap gbaseSwap) {
        this.gbaseSwap = gbaseSwap;
    }
}
