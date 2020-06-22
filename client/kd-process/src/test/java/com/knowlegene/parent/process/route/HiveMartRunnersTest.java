package com.knowlegene.parent.process.route;

import com.knowlegene.parent.process.route.fun.FunTest;
import com.knowlegene.parent.process.runners.JobRunners;

/**
 * @Author: limeng
 * @Date: 2019/9/18 14:09
 */
public class HiveMartRunnersTest extends JobRunners {
    @Override
    public void setJobStream() {
        this.defaultMethod(FunTest.class);
    }

    public static void main(String[] args) {
        HiveMartRunnersTest ht = new HiveMartRunnersTest();
        ht.start();
        ht.run();
    }

}
