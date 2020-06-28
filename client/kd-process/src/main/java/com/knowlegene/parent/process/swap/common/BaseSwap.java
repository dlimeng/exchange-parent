package com.knowlegene.parent.process.swap.common;



import com.knowlegene.parent.process.io.es.ESSwap;
import com.knowlegene.parent.process.io.es.impl.ESSwapImpl;
import com.knowlegene.parent.process.io.jdbc.GbaseSwap;
import com.knowlegene.parent.process.io.jdbc.HiveSwap;
import com.knowlegene.parent.process.io.jdbc.MySQLSwap;
import com.knowlegene.parent.process.io.jdbc.OracleSwap;
import com.knowlegene.parent.process.io.jdbc.impl.*;
import com.knowlegene.parent.process.io.neo4j.Neo4jSwap;
import com.knowlegene.parent.process.io.neo4j.impl.Neo4jSwapImpl;
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
    private static Neo4jSwap neo4jSwap;


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
        neo4jSwap = new Neo4jSwapImpl();

    }

    public static HiveSwap getHiveSwapImport(){
        return hiveSwapImport;
    }

    public static HiveSwap getHiveSwapExport(){
        return hiveSwapExport;
    }

    public static MySQLSwap getMySQLSwapImport(){
        return mySQLSwapImport;
    }

    public static MySQLSwap getMySQLSwapExport(){
        return mySQLSwapExport;
    }


    public static OracleSwap getOracleSwapImport() {
        return oracleSwapImport;
    }

    public static OracleSwap getOracleSwapExport() {
        return oracleSwapExport;
    }

    public static GbaseSwap getGbaseSwapImport() {
        return gbaseSwapImport;
    }

    public static GbaseSwap getGbaseSwapExport() {
        return gbaseSwapExport;
    }


    public static ESSwap getESSwap(){
        return eSSwap;
    }

    public static Neo4jSwap getNeo4jSwap(){
        return neo4jSwap;
    }



}
