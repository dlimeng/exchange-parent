package com.knowlegene.parent.cetl.hivemart;

import com.knowlegene.parent.cetl.hivemart.fun.EntityFun;
import com.knowlegene.parent.process.runners.JobRunners;

/**
 * @Author: limeng
 * @Date: 2019/8/13 18:43
 */
public class HiveMartRunners extends JobRunners {
    @Override
    public void setJobStream() {
        //根据kw-core-parent hive_mart 注册模块
//        this.defaultMethod(BusInfoDDL.class);
//        this.defaultMethod(BusInfoETL.class);
//
//        this.defaultMethod(EntityDDL.class);
//        this.defaultMethod(EntityETL.class);
//
//        this.defaultMethod(RelationDDL.class);
//        this.defaultMethod(RelationETL.class);
        //自定义函数
        this.defaultMethod(EntityFun.class);
    }

    public static void main(String[] args) {
        HiveMartRunners hiveMartRunners = new HiveMartRunners();
        hiveMartRunners.start();
        hiveMartRunners.run();
    }
}
