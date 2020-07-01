package com.knowlegene.parent.process.swap.common;



import com.knowlegene.parent.process.io.es.ESSwap;
import com.knowlegene.parent.process.io.es.impl.ESSwapImpl;
import com.knowlegene.parent.process.io.jdbc.GbaseSwap;
import com.knowlegene.parent.process.io.jdbc.HiveSwap;
import com.knowlegene.parent.process.io.jdbc.MySQLSwap;
import com.knowlegene.parent.process.io.jdbc.OracleSwap;
import com.knowlegene.parent.process.io.jdbc.impl.*;
import com.knowlegene.parent.process.io.neo4j.Neo4jSwap;
import com.knowlegene.parent.process.io.neo4j.impl.Neo4jSwapExport;
import com.knowlegene.parent.process.io.neo4j.impl.Neo4jSwapImport;
import lombok.Data;


import javax.annotation.Resource;
import java.io.Serializable;

/**
 * 公共交换方法
 * @Author: limeng
 * @Date: 2019/8/26 15:17
 */
@Data
public class BaseSwap  implements Serializable {
    @Resource
    private static HiveSwap hiveSwapImport;
    @Resource
    private static HiveSwap hiveSwapExport;


    @Resource
    private static MySQLSwap mySQLSwapImport;
    @Resource
    private static MySQLSwap mySQLSwapExport;


    @Resource
    private static OracleSwap oracleSwapImport;

    private static OracleSwap oracleSwapExport;


    @Resource
    private static GbaseSwap gbaseSwapImport;
    @Resource
    private static GbaseSwap gbaseSwapExport;


    @Resource
    private static ESSwap eSSwap;

    @Resource
    private static Neo4jSwap neo4jSwapImport;

    @Resource
    private static Neo4jSwap neo4jSwapExport;


    public BaseSwap() {
        hiveSwapImport = new HiveSwapImport();
        hiveSwapExport = new HiveSwapExport();

        mySQLSwapImport = new MySQLSwapImport();
        mySQLSwapExport = new MySQLSwapExport();

        oracleSwapImport = new OracleSwapImport();
        oracleSwapExport = new OracleSwapExport();

        gbaseSwapImport = new GbaseSwapImport();
        gbaseSwapExport = new GbaseSwapExport();

        eSSwap = new ESSwapImpl();

        neo4jSwapImport = new Neo4jSwapImport();
        neo4jSwapExport = new Neo4jSwapExport();
    }

    protected static HiveSwap getHiveSwapImport(){
        return hiveSwapImport;
    }

    protected static HiveSwap getHiveSwapExport(){
        return hiveSwapExport;
    }

    protected static MySQLSwap getMySQLSwapImport(){
        return mySQLSwapImport;
    }

    protected static MySQLSwap getMySQLSwapExport(){
        return mySQLSwapExport;
    }


    protected static OracleSwap getOracleSwapImport() {
        return oracleSwapImport;
    }

    protected static OracleSwap getOracleSwapExport() {
        return oracleSwapExport;
    }

    protected static GbaseSwap getGbaseSwapImport() {
        return gbaseSwapImport;
    }

    protected static GbaseSwap getGbaseSwapExport() {
        return gbaseSwapExport;
    }


    protected static ESSwap getESSwap(){
        return eSSwap;
    }



    protected static Neo4jSwap getNeo4jSwapImport() {
        return neo4jSwapImport;
    }

    protected static Neo4jSwap getNeo4jSwapExport() {
        return neo4jSwapExport;
    }
}
